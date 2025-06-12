package org.grapheco

import org.apache.arrow.flight.FlightProducer.{CallContext, ServerStreamListener, StreamListener}
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType.Int

import java.io.{File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class Dataset(schema: Schema, rows: Long) {
  // 暂时不加载数据，避免一次性加载到内存
  def getSchema: Schema = schema
  def getRows: Long = rows
}

class CookbookProducer(allocator: BufferAllocator, location: Location) extends NoOpFlightProducer {

  private val datasets = new ConcurrentHashMap[FlightDescriptor, Dataset]()

  val fields = List(
    new Field("id", FieldType.nullable(new Int(32, true)), null),
    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
          new Field("file", FieldType.nullable(new ArrowType.Binary()), null)
  )
  val schema = new Schema(fields.asJava)


  override def acceptPut(context: CallContext, flightStream: FlightStream, ackStream: StreamListener[PutResult]): Runnable = {
    // 逐个批次处理数据
    val schema = flightStream.getSchema
    var rows = 0L

    new Runnable {
      override def run(): Unit = {
        while (flightStream.next()) {
          val unloader = new VectorUnloader(flightStream.getRoot)
          val arb = unloader.getRecordBatch
          rows += flightStream.getRoot.getRowCount

          // 此处可以进行处理，比如写入数据库或文件，释放内存
          // 处理批次数据
          processBatch(arb)

          // 每处理完一个批次数据后，可以进行内存释放
          flightStream.getRoot.clear()
        }

        // 保存处理的元数据，避免加载所有数据
        val dataset = new Dataset(schema, rows)
        datasets.put(flightStream.getDescriptor, dataset)
        ackStream.onCompleted()
      }
    }
  }

  // 处理每一个数据批次，写入外部存储或者进行其他业务逻辑
  private def processBatch(batch: ArrowRecordBatch): Unit = {
    // 你可以在这里对每个批次数据进行处理，比如写入磁盘或数据库
//    println(s"Processing batch with ${batch.getLength} rows")
    // 例如：将批次数据写入文件
    // val fileWriter = new ArrowFileWriter(batch.getAllocator)
    // fileWriter.writeBatch(batch)
  }

  override def getStream(context: CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
    val dataset = datasets.get(flightDescriptor)
    if (dataset == null) {
      throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException()
    }

    val root = VectorSchemaRoot.create(schema, allocator)
    val loader = new VectorLoader(root)
    listener.start(root)

    // 假设数据已经写入外部存储，逐批返回
    // 此处可以模拟从外部存储中读取数据并逐批返回给客户端
    val batchs = 100
    for(i <- 0 until batchs){
      val dummyBatch = createDummyBatch() // 用模拟数据替代
      loader.load(dummyBatch)
      listener.putNext()
    }
    listener.completed()
  }

  // 模拟数据生成 (可以用实际从外部存储读取的方式替代)
  private def createDummyBatch(): ArrowRecordBatch = {

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val idVector = vectorSchemaRoot.getVector("id").asInstanceOf[IntVector]
    val nameVector = vectorSchemaRoot.getVector("name").asInstanceOf[VarCharVector]
    val fieldVector = vectorSchemaRoot.getVector("file").asInstanceOf[VarBinaryVector]
    val rows = 10 // 这里模拟10行数据
    idVector.allocateNew(rows)
    nameVector.allocateNew(rows)
    fieldVector.allocateNew(rows)
    for (i <- 0 until rows) {
      idVector.setSafe(i, i)
      val name = s"name_$i"
      nameVector.setSafe(i, name.getBytes("UTF-8"))
        fieldVector.setSafe(i,getFileByteArray("/Users/renhao/Downloads/数据导入核能修改.mov") )
    }
    vectorSchemaRoot.setRowCount(rows)


//    val vectorSchemaRoot = VectorSchemaRoot.create(new Schema(List(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)).asJava), allocator)
//    val varCharVector = vectorSchemaRoot.getVector("name").asInstanceOf[VarCharVector]
//    varCharVector.allocateNew(3)
//    varCharVector.set(0, "Ronald".getBytes())
//    varCharVector.set(1, "David".getBytes())
//    varCharVector.set(2, "Francisco".getBytes())
//    vectorSchemaRoot.setRowCount(3)

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

  private def getFileByteArray(filePath: String): Array[Byte] = {
    val file = new File(filePath)
    if (!file.exists() || !file.isFile) {
      throw new IOException(s"File not found or invalid: $filePath")
    }

    val inputStream = new FileInputStream(file)
    try {
      // 获取文件的字节数组
      val fileBytes = new Array[Byte](file.length().toInt)
      inputStream.read(fileBytes)
      fileBytes
    } finally {
      inputStream.close()
    }
  }

  override def doAction(context: CallContext, action: Action, listener: StreamListener[Result]): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(action.getBody, StandardCharsets.UTF_8))
    action.getType match {
      case "DELETE" =>
        val removed = datasets.remove(flightDescriptor)
        if (removed != null) {
          listener.onNext(new Result("Delete completed".getBytes(StandardCharsets.UTF_8)))
        } else {
          listener.onNext(new Result("Delete not completed. Reason: Key did not exist.".getBytes(StandardCharsets.UTF_8)))
        }
        listener.onCompleted()
      case _ => listener.onError(CallStatus.NOT_FOUND.toRuntimeException())
    }
  }

  override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
    new FlightInfo(datasets.get(descriptor).getSchema, descriptor, List(flightEndpoint).asJava, -1L, datasets.get(descriptor).getRows)
  }

  override def listFlights(context: CallContext, criteria: Criteria, listener: StreamListener[FlightInfo]): Unit = {
    datasets.forEach((k, v) => listener.onNext(getFlightInfo(null, k)))
    listener.onCompleted()
  }

  def close(): Unit = {
    datasets.forEach((_, dataset) => dataset)
  }
}

object FlightServerApp extends App {
  val location = Location.forGrpcInsecure("0.0.0.0", 33333)

  val allocator: BufferAllocator = new RootAllocator()

  try {
    val producer = new CookbookProducer(allocator, location)
    val flightServer = FlightServer.builder(allocator, location, producer).build()

    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
    flightServer.awaitTermination()
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

