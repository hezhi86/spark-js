package mapReduce

import org.apache.spark.sql.SparkSession

object ManyToOne {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlowCount")
      .master("local[1]")
      .getOrCreate()

    // Optional, help avoid errors due to long lineage
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("hdfs://127.0.0.1:9090/tmp/")

    /* 1.可以将path里的所有文件内容读出 */
    //spark.read.textFile(path)

    /* 2.指定文件列表 */
    val base = "hdfs://127.0.0.1:9090/user/ds/"
    val fileList = Array(base + "groupsort.txt", base + "flowcount.txt")

    /* union 连接 */
    val fileRDD  = fileList.map(spark.read.textFile(_))
    val lineData = fileRDD.reduce((x,y)=> x.union(y))
    lineData.show(20)

    spark.stop()
  }

}
