package mapReduce

import mapReduce.FlowPartitioner.preparation
import org.apache.spark.sql.SparkSession

object GroupSort {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlowCount")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    // Optional, help avoid errors due to long lineage
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("hdfs://127.0.0.1:9090/tmp/")

    val base = "hdfs://127.0.0.1:9090/user/ds/"
    val lines = spark.read.textFile(base + "groupsort.txt")

    /* 默认分区器是hashcode */
    val lineData = lines.map(preparation).repartition(3).toDF("itemid", "goodsid", "amount")
    val sortData = lineData.sortWithinPartitions("itemid", "amount")
    sortData.show(10)

    /* 取出金额最大的订单 */
    val maxData = sortData.groupBy("itemid").agg(max("amount")).sort($"itemid".asc)
    maxData.show(10)

    spark.stop()
  }

  def preparation(str: String): ItemBean = {
    //切分字段
    val fields = str.split(" ")
    //assert(fields.size == 4)

    //组装对象
    var itemid = fields(0).toString.trim
    var goodsid = fields(1).toString.trim
    var amount  = fields(2).toDouble
    ItemBean(itemid, goodsid, amount)
  }

}

case class ItemBean(itemid:String, goodsid:String, amount:Double)