package org.grapheco

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import java.nio.charset.StandardCharsets
import java.util.logging.Logger
import scala.collection.JavaConverters.seqAsJavaListConverter

object FlightClientApp extends App {

  val location = Location.forGrpcInsecure("0.0.0.0", 33333)

  val allocator: BufferAllocator = new RootAllocator()

  try {
    val flightClient = FlightClient.builder(allocator, location).build()

    // 1. Populate Data
    val schema = new Schema(List(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)).asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val varCharVector = vectorSchemaRoot.getVector("name").asInstanceOf[VarCharVector]

    varCharVector.allocateNew(3)
    varCharVector.set(0, "Ronald".getBytes())
    varCharVector.set(1, "David".getBytes())
    varCharVector.set(2, "Francisco".getBytes())
    vectorSchemaRoot.setRowCount(3)

    val listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener())
    listener.putNext()

    varCharVector.set(0, "Manuel".getBytes())
    varCharVector.set(1, "Felipe".getBytes())
    varCharVector.set(2, "JJ".getBytes())
    vectorSchemaRoot.setRowCount(3)

    listener.putNext()
    listener.completed()
    listener.getResult()
    println("Client (Populate Data): Wrote 2 batches with 3 rows each")
//
    // 2. Get metadata information
    val flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"))
    println(s"Client (Get Metadata): $flightInfo")

    // 3. Get data stream
    // 统计变量
    var totalRows: Long = 0
    var totalBytes: Long = 0
    val startTime = System.nanoTime()
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    var batch = 0

    try {
      val vectorSchemaRootReceived = flightStream.getRoot
//      ("Client (Get Stream):")
      while (flightStream.next()) {
        batch += 1
        println(s"Client Received batch #$batch, Data:")
        val rowCount = vectorSchemaRootReceived.getRowCount
        totalRows += rowCount
        val idVector = vectorSchemaRootReceived.getVector("id")
        val nameVector = vectorSchemaRootReceived.getVector("name")
        val fileVector = vectorSchemaRootReceived.getVector("file")

        for (i <- 0 until rowCount) {
          val id = idVector.asInstanceOf[IntVector].get(i)
          val nameBytes = nameVector.asInstanceOf[VarCharVector].get(i)
          val fileBytes: Array[Byte] = fileVector.asInstanceOf[VarBinaryVector].get(i)
          val name = new String(nameBytes, "UTF-8")
//                    println(s"Batch $batch: Row $i: id=$id, name=$name")
//          println(fileBytes.length)
          // 累计字节数（假设每个 id 占 4 字节，name 的字节数由其长度决定）
          totalBytes += 4 + nameBytes.length
          totalBytes += fileBytes.length
        }
      }
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
      flightStream.close()
    }

    // 4. Delete action
    val deleteActionResult = flightClient.doAction(new Action("DELETE", FlightDescriptor.path("profiles").getPath.get(0).getBytes(StandardCharsets.UTF_8)))
    while (deleteActionResult.hasNext) {
      val result = deleteActionResult.next()
      println(s"Client (Do Delete Action): ${new String(result.getBody, StandardCharsets.UTF_8)}")
    }

    // 5. List all flight info
    val flightInfos = flightClient.listFlights(Criteria.ALL)
    println("Client (List Flights Info) After delete:")
    flightInfos.forEach(info => println(info))

  } catch {
    case e: Exception => e.printStackTrace()
  }
}

