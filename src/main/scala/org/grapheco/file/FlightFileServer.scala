import org.apache.arrow.flight.FlightProducer.{CallContext, ServerStreamListener, StreamListener}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import java.io.{File, FileInputStream}
import scala.collection.JavaConverters._

object FlightFileServer extends App {
  val allocator = new RootAllocator(Long.MaxValue)
  val location = Location.forGrpcInsecure("localhost", 33333)

  val producer = new NoOpFlightProducer {
    override def getStream(context: CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
      val filePath = "/Users/renhao/Downloads/数据导入核能修改.mov" // 替换为你的实际文件路径
      val file = new File(filePath)
      if (!file.exists()) {
        System.err.println(s"File not found: $filePath")
        listener.error(CallStatus.NOT_FOUND.withDescription("File not found").toRuntimeException)
        return
      }

      val field = new Field("file_chunk", FieldType.nullable(new ArrowType.Binary()), null)
      val schema = new Schema(List(field).asJava)

      val root = VectorSchemaRoot.create(schema, allocator)
      val vector = root.getVector("file_chunk").asInstanceOf[VarBinaryVector]
      listener.start(root)

      val buffer = new Array[Byte](1 * 1024 * 1024) // 8MB buffer
      val input = new FileInputStream(file)

      try {
        var bytesRead = 0
        var rowCount = 0
        vector.allocateNew()

        while ({ bytesRead = input.read(buffer); bytesRead != -1 }) {
          vector.setSafe(rowCount, buffer, 0, bytesRead)
          rowCount += 1

          // 每 100 行 flush 一批数据
          if (rowCount >= 100) {
            root.setRowCount(rowCount)
            listener.putNext()
            vector.clear()
            rowCount = 0
          }
        }

        // flush 剩余行
        if (rowCount > 0) {
          root.setRowCount(rowCount)
          listener.putNext()
        }

        listener.completed()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          listener.error(CallStatus.INTERNAL.withDescription(e.getMessage).toRuntimeException)
      } finally {
        input.close()
        root.close()
      }
    }

    override def listFlights(context: CallContext, criteria: Criteria, listener: StreamListener[FlightInfo]): Unit = {}

    override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = {
      val schema = new Schema(List(new Field("file_chunk", FieldType.nullable(new ArrowType.Binary()), null)).asJava)
      val endpoint = new FlightEndpoint(new Ticket("file".getBytes), location)
      new FlightInfo(schema, descriptor, List(endpoint).asJava, -1, -1)
    }
  }

  val server = FlightServer.builder(allocator, location, producer).build()
  server.start()
  println("✅ Flight File Server started on port 33333")

  sys.addShutdownHook {
    println("🛑 Shutting down Flight server...")
    server.close()
    allocator.close()
  }

  server.awaitTermination()
}
