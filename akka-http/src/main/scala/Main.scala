
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.util.EntityUtils

import java.io.FileWriter
import java.io.File
import scala.io.Source

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

object MyHttpResponse {

  def getResponse(url: String, header: Map[String, String] = null): String = {
    val httpClient = HttpClientBuilder.create()
                     .setRedirectStrategy(new LaxRedirectStrategy()).build();

    //val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val get = new HttpGet(url)  //vs HttpPost

    if (header != null) {   // 设置 header
      header.keys.foreach { key =>
        get.setHeader(key, header(key))
      }
    }

    val response = httpClient.execute(get)    // 发送请求
    EntityUtils.toString(response.getEntity, "UTF-8")    // 获取返回结果
  }

  def getData(data: String): Unit = {
    var arr: Array[String] = data.split(",") 
    println(arr.length)

    var res = new Array[String](50)
    var len = 0
    for (i <- 0 to(arr.length - 1)) {
      if (arr(i).indexOf("qtyPurchased") > 0 || arr(i).indexOf("totalQty") > 0) {
        //.replace("\"", "")
        res(len) = arr(i)
        len += 1
        println(arr(i))
      } 
    }
    println(len +"_"+ res(0))
  }

  def writefile(str: String) {
    val pattern = new Regex("href=(.)</a></li>")
    val out = new FileWriter("/home/hank/Templates/spark-js/ebay.txt", true)

    out.write(str)
    out.close()

  }

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

  def main(args: Array[String]): Unit = {
    val url = "https://www.ebay.com/b/Womens-Clothing/15724/bn_661783"
    val header = Map(
      "accept" -> "*/*",
      "origin" -> "https://www.ebay.com",
      "accept-language" -> "zh-CN,zh;q=0.8")

    println("This get response: ")
    val query: Array[String] = url.replace("https://www.ebay.com/", "").split("/")
    println(query(1))

    val result = getResponse(url, header)
    println(result.length)  
    println(result.indexOf("<link rel=\"canonical\""))
    println(result.indexOf("<!--M/--></ul></section><!--M/-->"))
    val data = result.substring(result.indexOf("<link rel=\"canonical\""), result.indexOf("<!--M/--></ul></section><!--M/-->"))
    
    //getData(data)
    //println(data)

    writefile(data)
    //hdfs()
  }

}