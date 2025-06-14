package org.grapheco.spark

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{FieldVector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.com.google.common.io.Closeables
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

import java.io.{DataInputStream, File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.nio.file.{Files, Paths}
import java.util
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.reflect.internal.util.TableDef.Column
import scala.util.{Try, Using}
//import scala.jdk.CollectionConverters.{IteratorHasAsScala, _}
//import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:36
 * @Modified By:
 */
class SparkServer(allocator: BufferAllocator, location: Location) extends NoOpFlightProducer {

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, RemoteDataFrameImpl]()
  private val spark = SparkSession.builder()
    .appName("Spark Arrow Example")
    .master("local[*]")
    .getOrCreate()

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        while (flightStream.next()) {
          val root = flightStream.getRoot
          val rowCount = root.getRowCount
          val source = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
          val dfOperations: List[DFOperation] = List.range(0, rowCount).map(index => {
            val bytes = root.getFieldVectors.get(1).asInstanceOf[VarBinaryVector].get(index)
            if (bytes == null) null else
              SimpleSerializer.deserialize(bytes).asInstanceOf[DFOperation]
          })
          val remoteDataFrameImpl = if (dfOperations.contains(null)) RemoteDataFrameImpl(source = source, List.empty)
          else RemoteDataFrameImpl(source = source, ops = dfOperations)
          requestMap.put(flightStream.getDescriptor, remoteDataFrameImpl)
          flightStream.getRoot.clear()
        }
        ackStream.onCompleted()
      }
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
    val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)
//    val conf: SparkConf = new SparkConf()
//      .setAppName("Spark Data Source")
//      .setMaster("local[*]")
//      .set("spark.driver.memory", "1g")
//    val spark = SparkSession.builder()
//      .config(conf)
//      .getOrCreate()

    // 0. 根据来源类型判断是CSV文件还是图片目录
    val dataPath = request.source
    val isImageSource = detectImageSource(dataPath)
    println(isImageSource)
    // 1. 从本地加载数据
    var df = if (isImageSource) {
      // 加载图片数据集
      spark.read.format("binaryFile")
        .load(dataPath)
        .select(
          col("path").as("id"),
          //          col("content").as("name")
        )
    } else {
      // 加载CSV数据集
      spark.read.option("header", "false")
        .csv(dataPath)
        .select(
          col("_c0").as("id"),
          col("_c1").as("name")
        )
    }


    // 2. 应用操作（过滤、处理等）
    var resultDF = df
    request.ops.foreach(opt => {
      opt match {
        case filter@FilterOp(f) => df = df.filter(f(_))
        case m@MapOp(f) => df = spark.createDataFrame(df.rdd.map(f(_)), df.schema)
        case s@SelectOp(cols) => df = df.select(cols.map(col): _*)
        case l@LimitOp(n) => df = df.limit(n)
        case r@ReduceOp(f) => df.reduce((r1, r2) => f((r1, r2)))
        case _ => throw new Exception(s"${opt.toString} the Transformation is not supported")
      }
    })

    //    println(df.show(0))

    // 3. 根据数据类型创建不同的Arrow Schema
    val arrowSchema = if (isImageSource) {
      // 图片数据: id (string), data (binary)
      new Schema(List(
        new Field("id", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("name", FieldType.nullable(new ArrowType.Binary()), null)
      ).asJava)
    } else {
      // CSV数据: id (string), name (string)
      new Schema(List(
        new Field("id", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
      ).asJava)
    }

    if (isImageSource)
      getBinaryStream(spark, dataPath, arrowSchema, listener)
    else {
      // 4. 分批发送数据
      val schema = sparkSchemaToArrowSchema(df.schema)
      val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(schema, childAllocator)
      try {
        val loader = new VectorLoader(root)
        listener.start(root)
        //每1000条row为一批进行传输,将DataFrame转化成Iterator，不会一次性加载到内存
        df.toLocalIterator().asScala.grouped(1000).foreach(rows => {
          val batch = createDummyBatch(root, rows)
          try {
            loader.load(batch)
            listener.putNext()
          } finally {
            batch.close()
          }
        })
        listener.completed()
      } catch {
        case e: Throwable => listener.error(e)
          throw e
      } finally {
        if (root != null) root.close()
        if (childAllocator != null) childAllocator.close()
        requestMap.remove(flightDescriptor)
      }
    }

  }

  private def getBinaryStream(original_spark: SparkSession, dataPath: String, schema: Schema, listener: FlightProducer.ServerStreamListener): Unit = {
    // 读取文件为 [文件名, 二进制流] 的 RDD

    //    val binaryRDD = spark.sparkContext.binaryFiles(dataPath + "1.cram")
    //        println(s"文件数量: ${binaryRDD.count()}")
    original_spark.close()
    val spark = SparkSession.builder()
      .appName("LocalBinaryFileChunkReader")
      .master("local[*]") // 设置为本地模式，使用1个核心
      .getOrCreate()

    try {
      val chunkSize = 128 * 1024 * 1024 // 128MB
      val root = VectorSchemaRoot.create(schema, allocator)
      val loader = new VectorLoader(root)
      listener.start(root)
      println("getBinaryStream...")
      val fileStreams = readFileWithSpark(spark, dataPath)

      // 阶段2: 串行处理每个分块
      processFileChunks(spark, dataPath, chunkSize,fileStreams, schema, loader, listener)
      //      processLocalFileInChunks(spark, filePath, chunkSize,loader,schema,listener)
    } finally {

      listener.completed()
      spark.close()
      System.gc()
    }


  }

  private def processLocalFileInChunks(
                                        spark: SparkSession,
                                        filePath: String,
                                        chunkSize: Int,
                                        loader: VectorLoader,
                                        schema: Schema,
                                        listener: FlightProducer.ServerStreamListener
                                      ): Unit = {

    val file = new File(filePath)
    if (!file.exists()) {
      println(s"错误: 文件 $filePath 不存在!")
      return
    }

    val fileSize = file.length()
    val totalChunks = Math.ceil(fileSize.toDouble / chunkSize).toInt

    println(s"处理文件: ${file.getName}")
    println(s"  大小: ${fileSize / (1024.0 * 1024.0)} MB")
    println(s"  分块大小: ${chunkSize / (1024.0 * 1024.0)} MB")
    println(s"  总分块数: $totalChunks")

    val stream = new FileInputStream(file)

    try {
      for (chunkIndex <- 0 until totalChunks) {
        val startByte = chunkIndex.toLong * chunkSize
        val remainingBytes = fileSize - startByte
        val currentChunkSize = Math.min(chunkSize, remainingBytes).toInt

        println(s"\n处理分块 #${chunkIndex + 1}/${totalChunks} [${
          startByte / (1024.0 * 1024.0)
        } MB - ${
          (startByte + currentChunkSize) / (1024.0 * 1024.0)
        } MB]")

        // 读取当前分块
        val chunkData = new Array[Byte](currentChunkSize)
        stream.read(chunkData)

        // 处理数据
        sendChunkToFlight(schema, chunkData, loader, listener)

        // 显式清空引用，帮助GC
        chunkData.length
      }
      println("\n所有分块处理完成!")
    } finally {
      stream.close()
    }
  }

  /**
   * 第一阶段: 使用Spark读取文件（但不处理内容）
   */
  private def readFileWithSpark(
                                 spark: SparkSession,
                                 filePath: String
                               ): Array[PortableDataStream] = {
    println("第一阶段: 使用Spark读取文件元数据")

    // 检查文件是否存在
    val path = Paths.get(filePath)
    if (!Files.exists(path)) {
      println(s"错误: 文件 $filePath 不存在!")
      System.exit(1)
    }

    // 使用Spark的binaryFiles获取文件流（但不读取文件内容）
    val rdd: RDD[(String, PortableDataStream)] = spark.sparkContext.binaryFiles(filePath)


    //    val fileStream = rdd.first()._2
    // 获取所有二进制文件的流
    val fileStreams: Array[PortableDataStream] = rdd.map(_._2).collect()
    //    val fileSize = fileStream.size
    println(s"Spark加载文件完成")
    println(s"  - 文件名称: ${path.getFileName}")
    //    println(f"  - 文件大小: ${fileSize / (1024.0 * 1024)}%.2f MB")

    fileStreams
  }

  /**
   * 第二阶段: 在Driver端串行处理文件分块
   */
  private def processFileChunks(
                                 spark: SparkSession,
                                 filePath: String,
                                 chunkSize: Int,
                                 fileStreams: Array[PortableDataStream],
                                 schema: Schema,
                                 loader: VectorLoader,
                                 listener: FlightProducer.ServerStreamListener
                               ): Unit = {
    println("\n第二阶段: 在Driver端串行处理文件分块")

    // 关闭Spark的隐式转换
    import spark.implicits._

    val file = new File(filePath+"//1.cram")
    val fileSize = file.length()
    val totalChunks = Math.ceil(fileSize.toDouble / chunkSize).toInt

    println(s"处理分块配置:")
    println(f"  - 分块大小: ${chunkSize / (1024.0 * 1024)}%.2f MB")
    println(s"  - 总分块数: $totalChunks")
    println(s"  - 方法: 在Driver端串行处理\n")

    val startTime = System.nanoTime()
    fileStreams.foreach { pds =>
      try {
        val inputStream: DataInputStream = pds.open()    // 此时才加载单个文件内容
        // 处理逻辑 (如读取/解析等)
        var currentPosition: Long = 0
        try {
          for (chunkIndex <- 0 until totalChunks) {
            val currentChunkSize = Math.min(chunkSize, fileSize - currentPosition).toInt
            val chunkBuffer = new Array[Byte](currentChunkSize)
            println(s"处理分块 #${chunkIndex + 1}/$totalChunks:")
            println(f"  - 起始位置: ${currentPosition / (1024.0 * 1024)}%.2f MB")
            println(f"  - 分块大小: ${currentChunkSize / (1024.0 * 1024)}%.2f MB")
            // 从当前位置读取整个分块
            inputStream.readFully(chunkBuffer)
            currentPosition += currentChunkSize

            // 处理当前分块
            //        processChunk(chunkBuffer, chunkIndex, chunkStart, currentChunkSize)
            sendChunkToFlight(schema, chunkBuffer, loader, listener)
            // 显示进度信息
            val progress = (chunkIndex + 1) * 100.0 / totalChunks
            val elapsedMillis = (System.nanoTime() - startTime) / 1e6
            //        val speed = if (elapsedMillis > 0) {
            //          val mbProcessed = (chunkStart + currentChunkSize) / (1024.0 * 1024)
            //          f"${mbProcessed / (elapsedMillis / 1000)}%.2f MB/s"
            //        } else "N/A"

            println(f"  - 进度: ${progress}%.1f%%")
            //        println(s"  - 处理速度: $speed")
            println("---------------------------------")
          }


          println("\n所有分块处理完成!")
      } finally {
          inputStream.close()        // 确保资源释放
      }
    }

    }
  }


  private def sendChunkToFlight(
                                 schema: Schema,
                                 chunk: Array[Byte],
                                 loader: VectorLoader,
                                 listener: FlightProducer.ServerStreamListener
                               ): Unit = {
    val recordBatch = createDummyBatch(schema, chunk)
    try {
      loader.load(recordBatch)
      listener.putNext()
    } finally {
      recordBatch.close()
      System.gc()
    }
  }


  // 检测是否为图片数据源
  private def detectImageSource(path: String): Boolean = {
    val file = new File(path)
    if (file.isDirectory) {
      // 检查目录中是否包含图片文件
      file.listFiles().take(1).exists(f =>
        f.getName.endsWith(".jpg") ||
          f.getName.endsWith(".png") ||
          f.getName.endsWith(".jpeg") ||
          f.getName.endsWith(".cram")
      )
    } else {
      // 检查文件扩展名
      path.endsWith(".jpg") ||
        path.endsWith(".png") ||
        path.endsWith(".jpeg") ||
        path.endsWith(".cram")
    }
  }


  private def createDummyBatch(schema: Schema, chunk: Array[Byte]): ArrowRecordBatch = {
    //    val allocator: BufferAllocator = new RootAllocator()
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val idVector = vectorSchemaRoot.getVector("id").asInstanceOf[VarCharVector]

    //    val nameVector = getDataVector(schema,VectorSchemaRoot)
    val rowsLen = 1
    idVector.allocateNew(rowsLen)
    val nameVector = {
      val vec = vectorSchemaRoot.getVector("name")
      val v = vec.asInstanceOf[VarBinaryVector]
      v.allocateNew(rowsLen)
      vec
    }

    for (i <- 0 until rowsLen) {
      //      println(rows(i).get(0) + rows(i).get(1).toString)
      idVector.setSafe(i, i.toString.getBytes("UTF-8"))
      //      nameVector.setSafe(i, name.getBytes("UTF-8"))
      //      println(rows(i).getAs[Array[Byte]](1).mkString("Array(", ", ", ")"))
      nameVector match {
        case v: VarBinaryVector => v.setSafe(i, chunk)
      }
    }

    vectorSchemaRoot.setRowCount(rowsLen)


    // Collect ArrowFieldNode objects (unchanged)
    val fieldNodes = vectorSchemaRoot.getFieldVectors.asScala.map { fieldVector =>
      new ArrowFieldNode(fieldVector.getValueCount, 0) // 0 is the null count, adjust as needed
    }.toList.asJava

    // Collect ArrowBuf objects for each FieldVector's data
    val buffers = vectorSchemaRoot.getFieldVectors.asScala.flatMap { fieldVector =>
      // Get all ArrowBufs associated with the FieldVector
      fieldVector.getBuffers(true)
    }.toList.asJava

    // Create the ArrowRecordBatch
    new ArrowRecordBatch(vectorSchemaRoot.getRowCount, fieldNodes, buffers)
  }


  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
    new FlightInfo(new Schema(List.empty.asJava), descriptor, List(flightEndpoint).asJava, -1L, 0L)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    requestMap.forEach {
      (k, v) => listener.onNext(getFlightInfo(null, k))
    }
    listener.onCompleted()
  }

  def close(): Unit = spark.close()

  private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[org.apache.spark.sql.Row]): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    val fieldVectors = arrowRoot.getFieldVectors.asScala
    for (i <- rows.indices) {
      val row = rows(i)
      for (j <- row.schema.fields.indices) {
        val value = row.get(j)
        val vec = fieldVectors(j)
        // 支持基本类型处理（可扩展）
        value match {
          case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
          case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
          case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
          case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
          case v: String =>
            val bytes = v.getBytes("UTF-8")
            vec.asInstanceOf[VarCharVector].setSafe(i, bytes, 0, bytes.length)
          case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
          case null => vec.setNull(i)
          case _ => throw new UnsupportedOperationException("Type not supported")
        }
      }
    }
    arrowRoot.setRowCount(rows.length)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }

  private def sparkSchemaToArrowSchema(sparkSchema: StructType): Schema = {
    val fields: List[Field] = sparkSchema.fields.map { field =>
      val arrowFieldType = field.dataType match {
        case IntegerType =>
          new FieldType(field.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(field.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(field.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(field.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(field.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(field.nullable, ArrowType.Bool.INSTANCE, null)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${field.dataType}")
      }

      new Field(field.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
  }
}

object FlightServerApp extends App {
  val location = Location.forGrpcInsecure("0.0.0.0", 33333)

  val allocator: BufferAllocator = new RootAllocator()

  try {
    val producer = new SparkServer(allocator, location)
    val flightServer = FlightServer.builder(allocator, location, producer).build()

    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        flightServer.close()
        producer.close()
      }
    })
    flightServer.awaitTermination()
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

object sparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Arrow Example")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.csv("hdfs://10.0.82.139:8020/test/person_paper/part-00000").toDF("id", "name")
    df.show(10)
  }
}
