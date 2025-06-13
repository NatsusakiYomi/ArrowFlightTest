import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object SparkBinaryFileChunkReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BinaryFileProcessor")
      .master("local[*]")
      .config("spark.executor.memory", "1g")  // 限制执行器内存
      .getOrCreate()

    try {
      val filePath = "C:\\Users\\Yomi\\PycharmProjects\\ArrowFlightTest\\src\\main\\resources\\cram\\1.cram"
      val chunkSize = 128 * 1024 * 1024 // 128MB

      // 阶段1: 使用Spark读取文件
      val fileData = readFileWithSpark(spark, filePath)

      // 阶段2: 串行处理每个分块
      processFileChunks(spark, filePath, chunkSize, fileData)
    } finally {
      spark.stop()
    }
  }

  /**
   * 第一阶段: 使用Spark读取文件（但不处理内容）
   */
  private def readFileWithSpark(
                                 spark: SparkSession,
                                 filePath: String
                               ): PortableDataStream = {
    println("第一阶段: 使用Spark读取文件元数据")

    // 检查文件是否存在
    val path = Paths.get(filePath)
    if (!Files.exists(path)) {
      println(s"错误: 文件 $filePath 不存在!")
      System.exit(1)
    }

    // 使用Spark的binaryFiles获取文件流（但不读取文件内容）
    val rdd: RDD[(String, PortableDataStream)] = spark.sparkContext.binaryFiles(filePath)

    // 由于只有一个文件，我们可以安全获取第一条记录
    val fileStream = rdd.first()._2

//    val fileSize = fileStream.size
    println(s"Spark加载文件完成")
    println(s"  - 文件名称: ${path.getFileName}")
//    println(f"  - 文件大小: ${fileSize / (1024.0 * 1024)}%.2f MB")

    fileStream
  }

  /**
   * 第二阶段: 在Driver端串行处理文件分块
   */
  private def processFileChunks(
                                 spark: SparkSession,
                                 filePath: String,
                                 chunkSize: Int,
                                 fileStream: PortableDataStream
                               ): Unit = {
    println("\n第二阶段: 在Driver端串行处理文件分块")

    // 关闭Spark的隐式转换
    import spark.implicits._

    val file = new File(filePath)
    val fileSize = file.length()
    val totalChunks = Math.ceil(fileSize.toDouble / chunkSize).toInt

    println(s"处理分块配置:")
    println(f"  - 分块大小: ${chunkSize / (1024.0 * 1024)}%.2f MB")
    println(s"  - 总分块数: $totalChunks")
    println(s"  - 方法: 在Driver端串行处理\n")

    val startTime = System.nanoTime()
    val inputStream = fileStream.open()
    var currentPosition: Long = 0
    try {
      for (chunkIndex <- 0 until totalChunks) {
        val currentChunkSize = Math.min(chunkSize, fileSize - currentPosition).toInt
        val chunkBuffer = new Array[Byte](currentChunkSize)
        println(s"处理分块 #${chunkIndex + 1}/$totalChunks:")
        println(f"  - 起始位置: ${currentPosition / (1024.0 * 1024)}%.2f MB")
        println(f"  - 分块大小: ${currentChunkSize / (1024.0 * 1024)}%.2f MB")
        // 从当前位置读取整个分块
        inputStream.readFully(chunkBuffer)
        currentPosition += currentChunkSize

        // 处理当前分块
//        processChunk(chunkBuffer, chunkIndex, chunkStart, currentChunkSize)

        // 显示进度信息
        val progress = (chunkIndex + 1) * 100.0 / totalChunks
        val elapsedMillis = (System.nanoTime() - startTime) / 1e6
//        val speed = if (elapsedMillis > 0) {
//          val mbProcessed = (chunkStart + currentChunkSize) / (1024.0 * 1024)
//          f"${mbProcessed / (elapsedMillis / 1000)}%.2f MB/s"
//        } else "N/A"

        println(f"  - 进度: ${progress}%.1f%%")
//        println(s"  - 处理速度: $speed")
        println("---------------------------------")
      }

      // 关闭流
      //    fileStream.close()

      println("\n所有分块处理完成!")
    }
  }

  /**
   * 分块处理逻辑
   */
  private def processChunk(
                            data: Array[Byte],
                            chunkIndex: Int,
                            startByte: Long,
                            chunkSize: Int
                          ): Unit = {
    // 示例: 计算并显示分块的摘要信息
    val headerCheck = if (data.length >= 4) {
      (data(0) & 0xFF) << 24 | (data(1) & 0xFF) << 16 |
        (data(2) & 0xFF) << 8 | (data(3) & 0xFF)
    } else -1

    val footerCheck = if (data.length >= 4) {
      val last = data.length - 4
      (data(last) & 0xFF) << 24 | (data(last+1) & 0xFF) << 16 |
        (data(last+2) & 0xFF) << 8 | (data(last+3) & 0xFF)
    } else -1

    println("  分块摘要:")
    println(s"    - 首4字节: 0x${headerCheck.toHexString.toUpperCase}")
    println(s"    - 末4字节: 0x${footerCheck.toHexString.toUpperCase}")
    println(s"    - SHA-256: ${computeSha256(data).substring(0, 16)}...")

    // 可选: 保存分块到单独文件 (仅用于调试/演示)
    // if (chunkIndex < 3) { // 仅保存前3个分块用于测试
    //   val chunkPath = Paths.get(s"chunk-$chunkIndex.bin")
    //   Files.write(chunkPath, data)
    //   println(s"  - 分块保存到: $chunkPath")
    // }

    // 显式触发GC
    System.gc()
  }

  /**
   * 计算数据的SHA-256哈希值
   */
  private def computeSha256(data: Array[Byte]): String = {
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(data).map("%02x".format(_)).mkString
  }
}
