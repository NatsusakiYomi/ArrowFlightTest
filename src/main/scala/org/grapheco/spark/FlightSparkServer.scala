package org.grapheco.spark

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.immutable.List
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.reflect.internal.util.TableDef.Column

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
    val data = Seq(
      (1, """{"name":"Alice","id":"1"}""", "zhang"),
      (2,"""{"name":"Bob","id":"1"}""", "wang"),
      (3,"""{"name":"Charlie","id":"1"}""", "zhao")
    )
    import spark.implicits._
    // 依据requeste信息构建DataFrame
//    var df: DataFrame = spark.createDataFrame(data).toDF("id", "name", "xing")
    //限制分区最大128mb 防止toLocalIterator拉取数据OOM .option("maxSplitBytes", 134217728)
//    hdfs://master1:8020/test/paper_conf/part-00000
//    person_paper/part-00000
    var df = spark.read.csv("hdfs://10.0.82.139:8020/test/person_paper/part-00000").toDF("id","name")

    request.ops.foreach(opt => {
      opt match {
        case filter@FilterOp(f) => df = df.filter(f(_))
        case m@MapOp(f) => df = spark.createDataFrame(df.rdd.map(f(_)), df.schema)
        case s@SelectOp(cols)=> df = df.select(cols.map(col): _*)
        case l@LimitOp(n) => df = df.limit(n)
        case r@ReduceOp(f) => df.reduce((r1,r2) => f((r1,r2)))
        case _ => throw new Exception(s"${opt.toString} the Transformation is not supported")
      }
    })

    val schema = sparkSchemaToArrowSchema(df.schema)
    val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    try{
      val loader = new VectorLoader(root)
      listener.start(root)
      //每1000条row为一批进行传输,将DataFrame转化成Iterator，不会一次性加载到内存
      df.toLocalIterator().asScala.grouped(1000).foreach(rows => {
        val batch = createDummyBatch(root, rows)
        try{
          loader.load(batch)
          listener.putNext()
        }finally {
          batch.close()
        }
      })
      listener.completed()
    } catch {
      case e: Throwable => listener.error(e)
        throw e
    }finally {
      if (root != null) root.close()
      if (childAllocator != null) childAllocator.close()
      requestMap.remove(flightDescriptor)
    }
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

object sparkTest{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Arrow Example")
      .master("local[*]")
      .getOrCreate()
    val data = Seq(
      (1, """{"name":"Alice","id":"1"}"""),
      (2,"""{"name":"Bob","id":"1"}"""),
      (3,"""{"name":"Charlie","id":"1"}""")
    )
    // 依据requeste信息构建DataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF("id", "name")

    df.groupBy("name")
    val dff = spark.sql(
      """
        |
        |""".stripMargin)



  }
}
