package org.grapheco.spark

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.immutable.List
import scala.jdk.CollectionConverters.asScalaIteratorConverter

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

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
    val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)
    val spark = SparkSession.builder()
      .appName("Spark Arrow Example")
      .master("local[*]")
      .getOrCreate()
    val data = Seq(
      ("1", "Alice"),
      ("2", "Bob"),
      ("3", "Charlie")
    )
    // 依据requeste信息构建DataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF("id", "name")
    var result: DataFrame =null
    request.ops.foreach(opt => {
      opt match {
        case filter@FilterOp(f) => result = df.filter(row => {
         val b = f(row)
         b
        })
        case _ =>
      }
    })

    //限制分区最大128mb 防止toLocalIterator拉取数据OOM
//    val df = spark.read.option("maxSplitBytes", 134217728).csv("hdfs://10.0.82.139:8020/test/person_paper/part-00000").toDF("id","name")

    val fields: Seq[Field] = List(
      new Field("id", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    )
    val schema = new Schema(fields.asJava)
    val root = VectorSchemaRoot.create(schema, allocator)
    val loader = new VectorLoader(root)
    listener.start(root)
    //每1000条row为一批进行传输,将DataFrame转化成Iterator，不会一次性加载到内存
    result.toLocalIterator().asScala.grouped(1000).foreach(rows => {
      loader.load(createDummyBatch(schema, rows))
      listener.putNext()
    })
    listener.completed()
  }
  private def createDummyBatch(schema: Schema, rows: Seq[org.apache.spark.sql.Row]): ArrowRecordBatch = {
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val idVector = vectorSchemaRoot.getVector("id").asInstanceOf[VarCharVector]
    val nameVector = vectorSchemaRoot.getVector("name").asInstanceOf[VarCharVector]
    val rowsLen = rows.length
    idVector.allocateNew(rowsLen)
    nameVector.allocateNew(rowsLen)
    for (i <- 0 until rowsLen) {
//      println(rows(i).get(0) + rows(i).get(1).toString)
      idVector.setSafe(i, rows(i).get(0).asInstanceOf[String].getBytes("UTF-8"))
      val name = rows(i).get(1).asInstanceOf[String]
      nameVector.setSafe(i, name.getBytes("UTF-8"))
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
