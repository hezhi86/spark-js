
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object Movielens {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
           val fields = str.split("::")
           assert(fields.size == 4)
           Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
   }


  def main99(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Custmer_Statistics")
      .master("local[1]")
      .getOrCreate()
    // Optional, but may help avoid errors due to long lineage
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("hdfs://127.0.0.1:9090/tmp/")

    import spark.implicits._

    val base = "hdfs://127.0.0.1:9090/user/ds/"
    val ratings = spark.read.textFile(base +"sample_movielens_ratings.txt").map(parseRating).toDF()
    ratings.show(5)

    ratings.createOrReplaceTempView("input_data")
    val result_valuse = spark.sql("SELECT uts.userId,uts.movieId,uts.rating,uts.timestamp from(SELECT userId from input_data GROUP BY userId HAVING count(userId)>4) TABLE1 join input_data uts on (uts.userId = TABLE1.userId)")
    val ratings2 = result_valuse.toDF()

    //将数据集切分为训练集和测试集
    val Array(training, test) = ratings2.randomSplit(Array(0.9, 0.1))
    //建立推荐模型，一个是显性反馈，一个是隐性反馈
    val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01)
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setPredictionCol("prediction")
    val alsImplicit = new ALS().setMaxIter(5).setRegParam(0.01).setImplicitPrefs(true)
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setPredictionCol("prediction")

    //在训练数据上训练
    val modelExplicit = alsExplicit.fit(training)
    val modelImplicit = alsImplicit.fit(training)

    val predictionsExplicit = modelExplicit.transform(test)
    val predictionsImplicit = modelImplicit.transform(test)

    //推荐前100个主题
    val userRecs = modelExplicit.recommendForAllUsers(10)
    userRecs.show(20)
    val dataout = userRecs.rdd.map(x => x.get(0) + "\001" +  x.get(1)).take(20).foreach(println)

    spark.stop()
  }

}
