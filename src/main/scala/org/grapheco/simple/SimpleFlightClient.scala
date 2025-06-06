package org.grapheco.simple

import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object SimpleFlightClient {
  def main(args: Array[String]): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", 33333)).build()

    try {
      // 列出所有 Flight
      val flights = client.listFlights(Criteria.ALL).asScala.toList
      println(s"Available flights: ${flights.map(_.getDescriptor.getPath)}")

      val flight = flights.head
      val ticket = flight.getEndpoints.get(0).getTicket

      // 统计变量
      var totalRows: Long = 0
      var totalBytes: Long = 0

      // 记录开始时间（纳秒）
      val startTime = System.nanoTime()

      // 打开数据流
      val stream = client.getStream(ticket)
      val root = stream.getRoot
      while (stream.next()) {
        val rowCount = root.getRowCount
        totalRows += rowCount

        val idVector = root.getVector("id")
        val nameVector = root.getVector("name")
        val fileVector = root.getVector("file")

        for (i <- 0 until rowCount) {
          val id = idVector.asInstanceOf[IntVector].get(i)
          val nameBytes = nameVector.asInstanceOf[VarCharVector].get(i)
          val fileBytes: Array[Byte] = fileVector.asInstanceOf[VarBinaryVector].get(i)
          val name = new String(nameBytes, "UTF-8")
//          println(s"Row $i: id=$id, name=$name")

          // 累计字节数（假设每个 id 占 4 字节，name 的字节数由其长度决定）
          totalBytes += 4 + nameBytes.length
          totalBytes += fileBytes.length
        }
      }
      stream.close()

      // 记录结束时间
      val endTime = System.nanoTime()
      val durationSeconds = (endTime - startTime) / 1e9

      // 输出统计信息
      println(f"Total rows received: $totalRows")
      println(f"Total time: $durationSeconds%.3f seconds")

      if (durationSeconds > 0) {
        val throughputRows = totalRows / durationSeconds
        val throughputMB = (totalBytes / 1e6) / durationSeconds
        println(f"Throughput: $throughputRows%.2f rows/second")
        println(f"Data throughput: $throughputMB%.2f MB/second")
      }
    } finally {
      client.close()
      allocator.close()
    }
  }
}
