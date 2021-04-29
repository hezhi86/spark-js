package mapReduce

import org.apache.spark.sql.{Dataset, SparkSession}

object JoinFriends {

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
    val lineData = spark.read.textFile(base + "user_friends.txt")

    //1. (好友列表，用户)
    val userData = lineData.map(preparation).map(x => (x._1.split(","), x._2))
    userData.show()

    //2. (好友，用户)
    val frendTuple = userData
        .flatMap(x => {
          x._1.map((_, x._2))
        })

    //3. (好友，用户列表)
    val frendKey = frendTuple.rdd
        .map(t => (t._1, List(t._2)))
        .reduceByKey(_:::_)
        .filter(_._2.size >1)

    frendKey.foreach(println)
    println("+--------------------+")

    //4. (A-B用户，共同好友)
    val frendList = frendKey.flatMap(secondMapper)
    val result = frendList.map(t => (t._1, List(t._2))).reduceByKey(_:::_)
    result.foreach(println)

    spark.stop()
  }

  def preparation(str:String) : (String, String) = {
    //切分字段
    val data = str.split(":")
    val user = data(0)
    val friends = data(1)

    (friends, user)
  }

  def secondMapper(x : (String, List[String])) : Array[(String, String)]= {
    //用户名排序，再组成key
    var lis = x._2.sorted

    /*
      求出数组的长度，是一个数列的和
      length=2，则num=1
      length=4，则num=3+2+1=6 */
    var num = 0
    for (i <- 1 to lis.length - 1) {
      num += lis.length -i
    }
    var res = new Array[(String, String)](num)

    //数组下标
    var len = 0
    for(i <- 0 to lis.length -1) {
      for (j <- i to lis.length -2) {
        res(len) = (lis(i) +"-"+ lis(j+1), x._1) //两个用户间的共同好友
        len += 1
      }
    }

    res //return
  }
}
