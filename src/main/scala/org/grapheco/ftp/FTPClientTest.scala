package org.grapheco.ftp

import org.apache.commons.net.ftp.FTPClient
import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Date

object FtpSpeedTest {

  def uploadFile(ftpClient: FTPClient, localFilePath: String, remoteFilePath: String): Long = {
    val localFile = new File(localFilePath)
    val inputStream = new FileInputStream(localFile)

    try {
      // 开始计时
      val startTime = System.currentTimeMillis()

      // 上传文件
      ftpClient.storeFile(remoteFilePath, inputStream)

      // 结束计时
      val endTime = System.currentTimeMillis()

      val elapsedTime = endTime - startTime
      println(s"上传文件 ${localFile.getName} 完成，耗时: $elapsedTime 毫秒")
      elapsedTime
    } catch {
      case e: Exception =>
        e.printStackTrace()
        0L
    } finally {
      inputStream.close()
    }
  }

  def connectToFtpServer(host: String, port: Int, username: String, password: String): FTPClient = {
    val ftpClient = new FTPClient
    ftpClient.connect(host, port)
    ftpClient.login(username, password)
//    ftpClient.setFileType(FTPClient.)
    ftpClient.enterLocalPassiveMode()
    ftpClient
  }

  def main(args: Array[String]): Unit = {
    // FTP 服务器配置
    val host = "0.0.0.0"
    val port = 21
    val username = "xxx"
    val password = "xxx"

    // 本地文件路径和远程文件路径
    val localFilePath = "path/to/local/file.txt"
    val remoteFilePath = "remote/file.txt"

    val ftpClient = connectToFtpServer(host, port, username, password)

    try {
      // 上传文件并测试传输时间
      val uploadTime = uploadFile(ftpClient, localFilePath, remoteFilePath)

      if (uploadTime > 0) {
        val fileSize = new File(localFilePath).length()
        val speed = fileSize / (uploadTime / 1000.0) // 计算上传速度，单位是字节/秒
        println(f"上传速度: $speed%.2f 字节/秒")
      }
    } finally {
      ftpClient.disconnect()
    }
  }
}
