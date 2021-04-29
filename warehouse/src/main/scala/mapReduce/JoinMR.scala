package mapReduce

import org.apache.spark.sql.SparkSession

object JoinMR {

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
    val torder = spark.read.textFile(base + "t_order.txt")
    val tproduct = spark.read.textFile(base + "t_product.txt")

    val lineOrder = torder.map(preparation(_)).toDF("id", "date", "pid", "amount").createOrReplaceTempView("t_order")
    val lineProduct = tproduct.map(preparation(_)).toDF("id", "pname", "category_id", "price").createOrReplaceTempView("t_product")
    //spark.sqlContext.sql("select * from t_order").show(5)

    spark.sqlContext.sql(
      """
        select o.id order_id, o.date, o.amount, p.id p_id, p.pname, p.category_id, p.price
        from t_order o
        join t_product p on o.pid = p.id
      """).show(10)

    spark.stop()
  }

  def preparation(str:String) : Tdata = {
    //切分字段
    val fields = str.split(" ")
    //组装对象
    var id = fields(0).trim
    var t1 = fields(1).trim
    var t2 = fields(2).trim
    var t3 = fields(3).toLong

    Tdata(id, t1, t2, t3)
  }

}

//order: id:String, date:String, pid:String, amount:Long
//product: id:String, pname:String, categoryId:String, price:Long
case class Tdata(f0:String, f1:String, f2:String, amount:Long)
