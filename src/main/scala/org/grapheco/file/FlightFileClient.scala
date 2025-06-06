import org.apache.arrow.flight.{FlightClient, Location, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}

import java.io.FileOutputStream

object FlightFileClient extends App {
  val allocator = new RootAllocator(Long.MaxValue)
  val location = Location.forGrpcInsecure("localhost", 33333)
  val client = FlightClient.builder(allocator, location).build()

  val ticket = new Ticket("file".getBytes)
  val stream = client.getStream(ticket)
  val root = VectorSchemaRoot.create(stream.getSchema, allocator)
  val vector = root.getVector("file_chunk").asInstanceOf[VarBinaryVector]

  val outputPath = "/Users/renhao/Downloads/2.mov" // 替换为你的保存路径
  val output = new FileOutputStream(outputPath)

  var totalBytes: Long = 0
  val startTime = System.nanoTime()

  try {
    while (stream.next()) {
      val rowCount = root.getRowCount
      for (i <- 0 until rowCount) {
        val bytes = vector.get(i)
        if (bytes != null) {
          output.write(bytes)
          totalBytes += bytes.length
        }
      }
    }
  } catch {
    case e: Exception =>
      println(s"❌ Error during file download: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    try output.close() catch { case _: Throwable => () }
    try stream.close() catch { case _: Throwable => () }
    try client.close() catch { case _: Throwable => () }
    try root.close() catch { case _: Throwable => () }
    try allocator.close() catch { case _: Throwable => () }
  }

  val endTime = System.nanoTime()
  val duration = (endTime - startTime) / 1e9
  val throughputMBps = totalBytes / 1024.0 / 1024.0 / duration

  println(f"✅ Received ${totalBytes / 1024 / 1024} MB in $duration%.2f s")
  println(f"📈 Throughput: $throughputMBps%.2f MB/s")
}
