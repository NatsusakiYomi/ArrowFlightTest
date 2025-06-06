import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTP

import java.io.{ByteArrayOutputStream, InputStream}
import scala.collection.mutable.ArrayBuffer

object FtpDownloadTest {

  def downloadFile(ftpClient: FTPClient, remoteFilePath: String): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()

    // 获取文件的输入流
    val inputStream: InputStream = ftpClient.retrieveFileStream(remoteFilePath)

    try {
      // 下载文件并写入到 ByteArrayOutputStream
      val buffer = new Array[Byte](1024)   // 定义缓冲区
      var bytesRead: Int = 0

      // 读取并写入字节数组
      while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
        byteArrayOutputStream.write(buffer, 0, bytesRead)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      inputStream.close()
    }

    // 返回字节数组
    byteArrayOutputStream.toByteArray
  }

  def connectToFtpServer(host: String, port: Int, username: String, password: String): FTPClient = {
    val ftpClient = new FTPClient
    ftpClient.connect(host, port)
    ftpClient.login(username, password)
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    ftpClient.enterLocalPassiveMode()
    ftpClient
  }

  def main(args: Array[String]): Unit = {
    val host = "10.0.82.143"
    val port = 33333
    val username = "xxx"
    val password = "xxx"
    val remoteFilePath = "nohup.out"

    val ftpClient = connectToFtpServer(host, port, username, password)

    try {
      // 记录开始时间
      val startTime = System.currentTimeMillis()

      // 获取文件字节数组
      val fileBytes = downloadFile(ftpClient, remoteFilePath)

      // 记录结束时间
      val endTime = System.currentTimeMillis()

      // 计算下载速度
      val elapsedTime = endTime - startTime // 毫秒
      val fileSizeInBytes = fileBytes.length
      val fileSizeInMB = fileSizeInBytes / (1024.0 * 1024.0) // 转换为MB
      val downloadSpeedMBps = fileSizeInMB / (elapsedTime / 1000.0) // MB每秒

      // 打印字节数组长度和下载速度
      println(s"耗时：$elapsedTime ms")
      println(s"文件字节数组长度: $fileSizeInBytes 字节")
      println(f"速度: $downloadSpeedMBps%.2f MB/s")

    } finally {
      ftpClient.logout()
      ftpClient.disconnect()
    }
  }
}
