package org.grapheco.spark

import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ArrowFileWriter, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{FieldVector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.JavaConverters.seqAsJavaListConverter

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

class FlightSparkClient(url: String, port: Int) {

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
    if (ops.length == 0) {
      DFOperationVector.allocateNew(1)
      vectorSchemaRoot.setRowCount(1)
    } else {
      DFOperationVector.allocateNew(ops.length)
      for (i <- 0 to ops.length - 1) {
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
    //    var batch = 0
    //    try{
    //      val vectorSchemaRootReceived = flightStream.getRoot
    //      while (flightStream.next()) {
    //        batch += 1
    //        println(s"Client Received data from spark batch #$batch, Data:")
    //        val rowCount = vectorSchemaRootReceived.getRowCount
    //        val idVector = vectorSchemaRootReceived.getVector("id")
    //        val nameVector = vectorSchemaRootReceived.getVector("name")
    //        for (i <- 0 until rowCount) {
    //          val id = idVector.asInstanceOf[IntVector].get(i)
    //          val nameBytes = nameVector.asInstanceOf[VarCharVector].get(i)
    //          val name = new String(nameBytes, "UTF-8")
    //          println(s"Batch $batch: Row $i: id=$id, name=$name")
    //        }
    //      }
    //    }finally {
    //      flightStream.close()
    //    }
    //    try {
    new Iterator[Seq[Row]] {
      override def hasNext: Boolean = flightStream.next()

      override def next(): Seq[Row] = {
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val idVector = vectorSchemaRootReceived.getVector("id")
        val nameVector = vectorSchemaRootReceived.getVector("name")
        val nameHandler: Int => String = nameVector match {
          case v: VarCharVector =>
            // 字符串类型直接获取
            index => v.getObject(index).toString

          case v: VarBinaryVector =>
            // 二进制类型转换为Base64字符串（或其他处理）
            index => {
              val binaryData = v.get(index)
              // 转为Base64字符串（或需要时转为十六进制）
              java.util.Base64.getEncoder.encodeToString(binaryData)
            }

          case _ =>
            throw new IllegalStateException(
              s"Unsupported vector type for 'name': ${nameVector.getClass.getSimpleName}"
            )
        }
        Seq.range(0, rowCount).map(index => {
          val idBytes = idVector.asInstanceOf[VarCharVector].get(index)
//          val nameBytes = nameVector.asInstanceOf[VarCharVector].get(index)
          val name = nameHandler(index)
          val id = new String(idBytes, "UTF-8")
          Row(Map("id" -> id, "name" -> name))
        })
      }
    }.flatMap(rows => rows)
    //    }finally {
    //      flightStream.close()
    //    }


  }

  def close(): Unit = {
    flightClient.close()
  }

}
