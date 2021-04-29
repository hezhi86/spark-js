package mapReduce

import org.apache.spark.sql.{Dataset, SparkSession}

object FlowPartitioner {

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
    val lines = spark.read.textFile(base + "flowcount.txt")

    /* 指定数据分区器 */
    val lineData = lines.map(line => (line.split(" ")(0), line)).rdd.partitionBy(new ProvincePartitioner(3))
    /* 生成DataSet, 持久化避免重复混洗 */
    val flowData = lineData.values.map(preparation).toDF("phone", "upFlow", "dFlow", "sumFlow").persist()
    flowData.show(5)

    spark.stop()
  }

  def preparation(str: String): FlowBean02 = {
    //切分字段
    val fields = str.split(" ")
    //assert(fields.size == 4)

    //组装对象
    var phone = fields(0).toString.trim
    var upFlow = fields(1).toLong
    var dFlow  = fields(2).toLong
    FlowBean02(phone, upFlow, dFlow, upFlow + dFlow)
  }

}

/*
 * phone:手机号,
 * upFlow:上行流量, dFlow:下行流量, sumFlow:流量合计
 */
case class FlowBean02(phone:String, upFlow:Long, dFlow:Long, sumFlow:Long)

import org.apache.spark.Partitioner

/*
 * 自定义partitioner
 * 根据手机号前缀分区
 */
class ProvincePartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    //手机号前3位
    val prefix = key.toString().substring(0, 3)
    //用分区个数取模
    prefix.toInt % num
  }
}
