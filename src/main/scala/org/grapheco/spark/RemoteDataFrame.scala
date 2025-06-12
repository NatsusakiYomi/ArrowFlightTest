package org.grapheco.spark

import org.apache.spark.sql.Row

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame extends Serializable {
  def filter(f: Row => Boolean): RemoteDataFrame
  def select(columns: String*): RemoteDataFrame
  def limit(n: Int): RemoteDataFrame
  def foreach(f: Row => Unit): Unit // 远程调用 + 拉取结果
  def collect(): List[Row]
}

sealed trait DFOperation extends Serializable

case class FilterOp(f: SerializableFunction[Row, Boolean]) extends DFOperation
case class SelectOp(cols: Seq[String]) extends DFOperation
case class LimitOp(n: Int) extends DFOperation
case object CollectOp extends DFOperation


case class RemoteDataFrameImpl(source: String, ops: List[DFOperation],remoteExecutor: RemoteExecutor = null) extends RemoteDataFrame {
  override def filter(f: Row => Boolean): RemoteDataFrame = {
    copy(ops = ops :+ FilterOp(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    }))
  }

  override def select(columns: String*): RemoteDataFrame = {
    copy(ops = ops :+ SelectOp(columns))
  }

  override def limit(n: Int): RemoteDataFrame = {
    copy(ops = ops :+ LimitOp(n))
  }

  override def foreach(f: Row => Unit): Unit = remoteExecutor.execute(source, ops).foreach(f)

  override def collect(): List[Row] = remoteExecutor.execute(source, ops).toList
}

object DacpClient{

  private def splitStr(ss: String): String = {
    println("-------------------------------------调用外部函数")
    val r = ss.split("i")(0)
    println("----------------" + r)
    r
  }

  private def printStr(ss: String): String = {
    println("-------------------------------------调用外部函数")
    val r = ss.split("[\\\\/]").last
    println("----------------" + r)
    r
  }


  def connect (url: String, port: Int): RemoteExecutor = new RemoteExecutor(url, port)

  def main(args: Array[String]): Unit = {
    val df: RemoteDataFrameImpl = connect("0.0.0.0", 33333).open("C:\\Users\\Yomi\\PycharmProjects\\ArrowFlightTest\\src\\main\\resources\\jpg")
    var num = 0
    df.filter(row => printStr(row.get(0).toString) == "cdCSj.jpg").foreach(row => {
      println(row)
//      num +=1
//      if(num % 5000 ==0){
//        println(s"传输数据共计：$num 条")
//      }
    })
  }
}

