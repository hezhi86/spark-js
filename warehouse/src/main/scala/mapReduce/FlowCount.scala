package mapReduce

import org.apache.spark.sql.{Dataset, SparkSession}

object FlowCount {

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

    /* 生成DataSet */
    val flowData = lines.map(preparation).toDF("phone", "upFlow", "dFlow", "sumFlow")
    flowData.show(5)

    /* 以手机号为key，对数值求和 */
    val sumData =  flowData.groupBy("phone").agg(sum("upFlow"),sum("dFlow"), sum("sumFlow"))
    sumData.show(5)

    spark.stop()
  }

  def preparation(str: String): FlowBean = {
    //切分字段
    val fields = str.split(" ")
    //assert(fields.size == 4)

    //组装对象
    var phone = fields(0).toString.trim
    var upFlow = fields(1).toLong
    var dFlow  = fields(2).toLong
    FlowBean(phone, upFlow, dFlow, upFlow + dFlow)
  }

}

/*
 * phone:手机号,
 * upFlow:上行流量, dFlow:下行流量, sumFlow:流量合计
 */
case class FlowBean(phone:String, upFlow:Long, dFlow:Long, sumFlow:Long)


