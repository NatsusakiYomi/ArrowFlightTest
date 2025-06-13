package org.grapheco.spark

import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ArrowFileWriter, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{FieldVector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}

import java.util.UUID
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:17
 * @Modified By:
 */


class RemoteExecutor(url: String, port: Int) {
  def execute(source: String, ops: List[DFOperation]): Iterator[Row] = {
    val client = new FlightSparkClient(url, port)
    client.getRows(source, ops)
  }
  def open(dataSource: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(dataSource, List.empty, this)
  }
}

class FlightSparkClient(url: String, port:Int) {

  val location = Location.forGrpcInsecure(url, port)
  val allocator: BufferAllocator = new RootAllocator()
  val flightClient = FlightClient.builder(allocator, location).build()

  def getRows(source: String, ops: List[DFOperation]): Iterator[Row] = {
    //上传参数
    val paramFields: Seq[Field] = List(
      new Field("source", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("DFOperation", FieldType.nullable(new ArrowType.Binary()), null)
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val varCharVector = vectorSchemaRoot.getVector("source").asInstanceOf[VarCharVector]
    val DFOperationVector = vectorSchemaRoot.getVector("DFOperation").asInstanceOf[VarBinaryVector]
    varCharVector.allocateNew(1)
    varCharVector.set(0, source.getBytes)
    if(ops.length == 0){
      DFOperationVector.allocateNew(1)
      vectorSchemaRoot.setRowCount(1)
    }else{
      DFOperationVector.allocateNew(ops.length)
      for(i <- 0 to ops.length -1){
        DFOperationVector.set(i, SimpleSerializer.serialize(ops(i)))
      }
      vectorSchemaRoot.setRowCount(ops.length)
    }

    val requestSchemaId = UUID.randomUUID().toString
    val listener = flightClient.startPut(FlightDescriptor.path(requestSchemaId), vectorSchemaRoot, new AsyncPutListener())
    listener.putNext()
    listener.completed()
    listener.getResult()
    //获取数据
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(requestSchemaId))
    //flightInfo 中可以获取schema
    println(s"Client (Get Metadata): $flightInfo")
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
      new Iterator[Seq[Row]] {
        override def hasNext: Boolean = flightStream.next()

        override def next(): Seq[Row] = {

          val vectorSchemaRootReceived = flightStream.getRoot
          val rowCount = vectorSchemaRootReceived.getRowCount
          val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
          Seq.range(0, rowCount).map(index => {
            val rowMap = fieldVectors.map(vec => {
              if(vec.isNull(index)) (vec.getName, null)
              else vec match {
                case v: org.apache.arrow.vector.IntVector     => (vec.getName, v.get(index))
                case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
                case v: org.apache.arrow.vector.Float8Vector  => (vec.getName, v.get(index))
                case v: org.apache.arrow.vector.BitVector     => (vec.getName, v.get(index) == 1)
                case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
              }
            }).toMap
            Row(rowMap.toSeq.map(x => x._2): _*)
          })
        }
      }.flatMap(rows => rows)
  }

  def close(): Unit = {
    flightClient.close()
  }

}
