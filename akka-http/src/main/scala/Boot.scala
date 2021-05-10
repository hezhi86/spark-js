import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

object Boot extends MultipartFormDataHandler {

  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  def hdfs() {
    val config = new Configuration()

    val hdfs = FileSystem.get(new URI("hdfs://10.100.163.16:9000"), config, "spark")
    val path = new Path("/home/NOTICE.txt")

    println("===========:"+hdfs.exists(path))

    if (hdfs.exists(path)) {
        val inputStream = hdfs.open(path)
        val stat = hdfs.getFileStatus(path)
        val length = stat.getLen.toInt
        println("===========1:"+stat)
        println("===========2:"+length)
      }

    hdfs.close()
  }

  def main99(args : Array[String]) : Unit = {

	  Http().bindAndHandle(routes, "0.0.0.0", 9000) map { result =>
	    println("Server has started on port 9000...")
	  } recover {
	    case ex: Exception => println(s"Server binding failed due to ${ex.getMessage}")
	  }
  }

}
