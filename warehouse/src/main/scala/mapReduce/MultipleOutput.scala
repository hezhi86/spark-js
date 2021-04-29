package mapReduce

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.sql.{Dataset, SparkSession}

object MultipleOutput {

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
    val lines = spark.read.textFile(base + "grouporder.txt")

    val lineData = lines.map(line => (line.split(" ")(0), line))
    lineData.show(10)

    /*
      reduceByKey, 对比 groupByKey , cogroup(rdd1, rdd2)
      第一步: 把value转成List
      第二步: 用连接符 ::: 或 ++ 连接List
      */
    val groupData = lineData.rdd
      .map(t => (t._1, List(t._2)))
      .reduceByKey(_:::_) //_++_
      .toDS()

    val multipleOutput = new MultipleOutput(spark)
    //保存文件
    multipleOutput.saveFile(groupData, base)

    spark.stop()
  }
}

/*
  foreachPartition 是在每个partition中把iterator传给function,让function自己对iterator进行处理（可以避免内存溢出）.
  foreachPartition 没返回值,并且是action操作.
  mapPartitions   有返回值,继续在返回RDD上做其他的操作，是Transformation操作会延迟执行, 用action方法来触发操作， 如，count、first
  spark的运算操作有两种类型：分别是Transformation和Action - https://blog.csdn.net/u010454030/article/details/78897150
*/

/*《Spark快速大数据分析》书中的例子: 例 10-28 */
/*
   rdd.foreachPartition { partition =>
   // 打开到存储系统的连接（比如一个数据库的连接）
   partition.foreach { item =>
   // 使用连接把item存到系统中
   }
   // 关闭连接
*/

class MultipleOutput(private val spark: SparkSession)  extends java.io.Serializable {

  /* using parallel, List.par.foreach{} */
  def saveFile(data : Dataset[(String, List[String])], base : String) : Unit = {

    data.collect().par.foreach(line => {
      println(line._1)
      //println(line._2)
      val rdd = spark.sparkContext.makeRDD(line._2)
      rdd.saveAsTextFile(base + line._1)
    })
  }
}

/*
  自定义文件名，
  第一步: 扩展类, class My... extends MultipleTextOutputFormat
  第二步: rdd.saveAsHadoopFile(..., classOf[MyTextOutputFormat]),
  转自-- https://blog.csdn.net/m0_37813354/article/details/103963978
 */
class MyTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    /* 命名规则 */
    key.asInstanceOf[String] + ".txt"
  }
  //rdd.saveAsHadoopFile("/user/output", classOf[String], classOf[String], classOf[MyTextOutputFormat])
}
