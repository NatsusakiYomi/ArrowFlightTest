package org.grapheco.simple

import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.ArrowType.Int
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.util.Collections
import java.util.concurrent.Executors
import scala.collection.JavaConverters.asJavaIterableConverter

object SimpleFlightServer {
  def main(args: Array[String]): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val port = 33333

    // 构造表结构：int32, utf8 两列
    val fields = List(
      new Field("id", FieldType.nullable(new Int(32, true)), null),
      new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    )
    val schema = new Schema(fields.asJava)

    // FlightProducer实现
    val producer = new FlightProducer {
      override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        val root = VectorSchemaRoot.create(schema, allocator)
        try {
          val idVector = root.getVector("id").asInstanceOf[IntVector]
          val nameVector = root.getVector("name").asInstanceOf[VarCharVector]

          root.allocateNew()

          val rows = 50000000 // 这里模拟50000000行数据
          for (i <- 0 until rows) {
            idVector.setSafe(i, i)
            val name = s"name_$i"
            nameVector.setSafe(i, name.getBytes("UTF-8"))
          }
          root.setRowCount(rows)

          listener.start(root)
          listener.putNext()
          listener.completed()
        } finally {
          root.close()
        }
      }

      override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
        val descriptor = FlightDescriptor.path("simple")
        val endpoint = new FlightEndpoint(new Ticket("simple-ticket".getBytes), List[Location](): _*)
        val info = new FlightInfo(schema, descriptor, Collections.singletonList(endpoint), -1L, -1L)
        listener.onNext(info)
        listener.onCompleted()
      }

      override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
        val endpoint = new FlightEndpoint(new Ticket("simple-ticket".getBytes), List[Location](): _*)
        new FlightInfo(schema, descriptor, Collections.singletonList(endpoint), -1L, -1L)
      }

      override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException()
      }

      override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
        listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException())
      }

      override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = {
        listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException())
      }
    }

    val server = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
      .executor(Executors.newFixedThreadPool(10))
      .build()

    server.start()
    println(s"Flight Server started on port $port")
    server.awaitTermination()
  }
}
