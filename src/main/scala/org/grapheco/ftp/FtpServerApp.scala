package org.grapheco.ftp

import org.apache.ftpserver.FtpServer
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.UserManager
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

object FtpServerApp {
  def main(args: Array[String]): Unit = {
    // 设置 FTP 服务器监听端口和协议
    val listenerFactory = new ListenerFactory()
    listenerFactory.setPort(21)  // 设置 FTP 端口

    // 设置用户管理
    val userManagerFactory = new PropertiesUserManagerFactory()
    val userManager: UserManager = userManagerFactory.createUserManager()

    // 创建一个 FTP 用户
    val user = new BaseUser()
    user.setName("xxx")         // 用户名
    user.setPassword("xxx") // 密码
    user.setHomeDirectory("/Users/renhao/Downloads") // 设置 FTP 根目录

    // 将用户添加到用户管理器中
    userManager.save(user)

    // 创建 FTP 服务器
    val ftpServerFactory = new FtpServerFactory()
    ftpServerFactory.addListener("default", listenerFactory.createListener())
    ftpServerFactory.setUserManager(userManager)

    // 启动 FTP 服务器
    val ftpServer: FtpServer = ftpServerFactory.createServer()
    println("FTP 服务器启动中...")
    ftpServer.start()

  }
}

