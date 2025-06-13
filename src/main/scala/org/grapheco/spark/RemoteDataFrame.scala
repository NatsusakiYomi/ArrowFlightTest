package org.grapheco.spark

import org.apache.spark.sql.Row
import org.json.JSONObject

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame extends Serializable {
  def map(f: Row => Row): RemoteDataFrame
  def filter(f: Row => Boolean): RemoteDataFrame
  def select(columns: String*): RemoteDataFrame
  def limit(n: Int): RemoteDataFrame

  def reduce(f: ((Row, Row)) => Row): RemoteDataFrame

  def foreach(f: Row => Unit): Unit // 远程调用 + 拉取结果
  def collect(): List[Row]
}

sealed trait DFOperation extends Serializable

case class MapOp(f: SerializableFunction[Row, Row]) extends DFOperation
case class FilterOp(f: SerializableFunction[Row, Boolean]) extends DFOperation
case class SelectOp(cols: Seq[String]) extends DFOperation
case class LimitOp(n: Int) extends DFOperation

case class ReduceOp(f: SerializableFunction[(Row, Row), Row]) extends DFOperation
case class MaxOp(column: String) extends DFOperation
case class GroupByOp(column: String) extends DFOperation

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrameImpl){
  def max(column: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(remoteDataFrameImpl.source, remoteDataFrameImpl.ops :+ MaxOp(column), remoteDataFrameImpl.remoteExecutor)
  }
  //可自定义聚合函数
}

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

  override def map(f: Row => Row): RemoteDataFrame = {
    copy(ops = ops :+ MapOp(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    }))
  }

  override def reduce(f: ((Row, Row)) => Row): RemoteDataFrame = {
    copy(ops = ops :+ ReduceOp(new SerializableFunction[(Row, Row), Row] {
      override def apply(v1: (Row, Row)): Row = f(v1)
    }))
  }

  def groupBy(column: String): GroupedDataFrame = {
    copy(ops = ops :+ GroupByOp(column))
    GroupedDataFrame(this)
  }
}

object DacpClient{

  private def splitStr(ss: String): String = {
    val r = new JSONObject(ss).get("name").toString.split("i")(0)
    r
  }

  def connect (url: String, port: Int): RemoteExecutor = new RemoteExecutor(url, port)

  def main(args: Array[String]): Unit = {
    val df: RemoteDataFrameImpl = connect("0.0.0.0", 33333).open("test")
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 500000
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    df.foreach(row => {
//      计算当前 row 占用的字节数（UTF-8 编码）
      val bytesLen =
        row.get(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length +
          row.get(1).asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length

      totalBytes += bytesLen
      realBytes += bytesLen

      count += 1

      if (count % batchSize == 0) {
        val endTime = System.currentTimeMillis()
        val real_elapsedSeconds = (endTime - start).toDouble / 1000
        val total_elapsedSeconds = (endTime - startTime).toDouble / 1000
        val real_mbReceived = realBytes.toDouble / (1024 * 1024)
        val total_mbReceived = totalBytes.toDouble / (1024 * 1024)
        val bps = real_mbReceived / real_elapsedSeconds
        val obps = total_mbReceived / total_elapsedSeconds
        println(f"Received: $count rows, total: $total_mbReceived%.2f MB, speed: $bps%.2f MB/s, overall speed: $obps%.2f MB/s")
        start = System.currentTimeMillis()
        realBytes = 0L
      }
    })
  }
}

