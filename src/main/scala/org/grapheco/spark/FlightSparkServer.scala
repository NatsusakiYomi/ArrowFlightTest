package org.grapheco.spark

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{FieldVector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, seqAsJavaListConverter}
import scala.collection.immutable.List
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

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        while (flightStream.next()){
          val root = flightStream.getRoot
          val rowCount = root.getRowCount
          val source = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
          val dfOperations: List[DFOperation] = List.range(0, rowCount).map(index => {
            val bytes = root.getFieldVectors.get(1).asInstanceOf[VarBinaryVector].get(index)
            if(bytes==null) null else
            SimpleSerializer.deserialize(bytes).asInstanceOf[DFOperation]
          })
          val remoteDataFrameImpl = if(dfOperations.contains(null)) RemoteDataFrameImpl(source = source, List.empty)
            else RemoteDataFrameImpl(source = source, ops = dfOperations)
          requestMap.put(flightStream.getDescriptor, remoteDataFrameImpl)
          flightStream.getRoot.clear()
        }
        ackStream.onCompleted()
      }
    }
  }

//  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
//    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
//    val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)
//    val spark = SparkSession.builder()
//      .appName("Spark Arrow Example")
//      .master("local[*]")
//      .getOrCreate()
//    val data = Seq(
//      ("1", "Alice"),
//      ("2", "Bob"),
//      ("3", "Charlie")
//    )
//    // 依据requeste信息构建DataFrame
//    val df: DataFrame = spark.createDataFrame(data).toDF("id", "name")
//    var result: DataFrame =null
//    request.ops.foreach(opt => {
//      opt match {
//        case filter@FilterOp(f) => result = df.filter(row => {
//         val b = f(row)
//         b
//        })
//        case _ =>
//      }
//    })
//
//    //限制分区最大128mb 防止toLocalIterator拉取数据OOM
////    val df = spark.read.option("maxSplitBytes", 134217728).csv("hdfs://10.0.82.139:8020/test/person_paper/part-00000").toDF("id","name")
//
//    val fields: Seq[Field] = List(
//      new Field("id", FieldType.nullable(new ArrowType.Utf8()), null),
//      new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
//    )
//    val schema = new Schema(fields.asJava)
//    val root = VectorSchemaRoot.create(schema, allocator)
//    val loader = new VectorLoader(root)
//    listener.start(root)
//    //每1000条row为一批进行传输,将DataFrame转化成Iterator，不会一次性加载到内存
//    result.toLocalIterator().asScala.grouped(1000).foreach(rows => {
//      loader.load(createDummyBatch(schema, rows))
//      listener.putNext()
//    })
//    listener.completed()
//  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
    val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)

    val spark = SparkSession.builder()
      .appName("Spark Data Source")
      .master("local[*]")
      .getOrCreate()

    // 0. 根据来源类型判断是CSV文件还是图片目录
    val dataPath = request.source
    val isImageSource = detectImageSource(dataPath)
    println(isImageSource)
    // 1. 从本地加载数据
    val df = if (isImageSource) {
      // 加载图片数据集
      spark.read.format("binaryFile")
        .load(dataPath)
        .select(
          col("path").as("id"),
          col("content").as("name"))
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
        case filter@FilterOp(f) =>
          resultDF = resultDF.filter(row => f(row))
        case _ =>
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

    // 4. 分批发送数据
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    listener.start(root)
    resultDF.toLocalIterator().asScala.grouped(1000).foreach(rows => {
      // 根据数据类型填充不同格式的数据
            loader.load(createDummyBatch(arrowSchema, rows))
            listener.putNext()
      listener.putNext()
    })

    listener.completed()
  }

  // 检测是否为图片数据源
  private def detectImageSource(path: String): Boolean = {
    val file = new File(path)
    if (file.isDirectory) {
      // 检查目录中是否包含图片文件
      file.listFiles().take(1).exists(f =>
        f.getName.endsWith(".jpg") ||
          f.getName.endsWith(".png") ||
          f.getName.endsWith(".jpeg")
      )
    } else {
      // 检查文件扩展名
      path.endsWith(".jpg") ||
        path.endsWith(".png") ||
        path.endsWith(".jpeg")
    }
  }

  private def createDummyBatch(schema: Schema, rows: Seq[org.apache.spark.sql.Row]): ArrowRecordBatch = {
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val idVector = vectorSchemaRoot.getVector("id").asInstanceOf[VarCharVector]

//    val nameVector = getDataVector(schema,VectorSchemaRoot)
    val rowsLen = rows.length
    idVector.allocateNew(rowsLen)

    lazy val nameVector = {
      val field = schema.findField("name")
      val vec = vectorSchemaRoot.getVector("name")
      field.getType match {
        case _: ArrowType.Utf8 =>
          val v = vec.asInstanceOf[VarCharVector]
          v.allocateNew(rows.size)
          v
        case _: ArrowType.Binary =>
          val v = vec.asInstanceOf[VarBinaryVector]
          v.allocateNew(rows.size)
          v
        case t => throw new IllegalArgumentException(s"Unsupported type: $t")
      }
    }

    for (i <- 0 until rowsLen) {
//      println(rows(i).get(0) + rows(i).get(1).toString)
      idVector.setSafe(i, rows(i).get(0).asInstanceOf[String].getBytes("UTF-8"))
//      nameVector.setSafe(i, name.getBytes("UTF-8"))
//      println(rows(i).getAs[Array[Byte]](1).mkString("Array(", ", ", ")"))
      nameVector match {
        case v: VarCharVector   => v.setSafe(i, rows(i).get(1).asInstanceOf[String].getBytes("UTF-8"))
        case v: VarBinaryVector => v.setSafe(i, rows(i).getAs[Array[Byte]](1))
      }
    }

    vectorSchemaRoot.setRowCount(rowsLen)


    // Collect ArrowFieldNode objects (unchanged)
    val fieldNodes = vectorSchemaRoot.getFieldVectors.asScala.map { fieldVector =>
      new ArrowFieldNode(fieldVector.getValueCount, 0)  // 0 is the null count, adjust as needed
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
    requestMap.forEach{
      (k, v) => listener.onNext(getFlightInfo(null, k))
    }
    listener.onCompleted()
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
    flightServer.awaitTermination()
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

object sparkTest{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Arrow Example")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.csv("hdfs://10.0.82.139:8020/test/person_paper/part-00000").toDF("id","name")
    df.show(10)
  }
}
