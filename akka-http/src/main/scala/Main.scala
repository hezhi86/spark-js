
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.util.EntityUtils

import scala.io.Source
import scala.util.matching.Regex

import java.io.FileWriter
import java.io.File


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

  def getData(url: String): Unit = {
    
    val header = Map(
      "accept" -> "*/*",
      "origin" -> "https://www.ebay.com",
      "accept-language" -> "zh-CN,zh;q=0.8")

    println(url)

    val result = getResponse(url, header)
    println(result.length)  
    println(result.indexOf("<link rel=\"canonical\""))
    println(result.indexOf("<!--M/--></ul></section><!--M/-->"))
    
    val data = result.substring(result.indexOf("<link rel=\"canonical\""), result.indexOf("<!--M/--></ul></section><!--M/-->"))

    var sp = result.split("ebay.com/itm/")
    sp(0) = ":"
    //println(sp(0))

    writefile(sp)
  }

  def getDll(url: String): Unit = {

    val header = Map(
      "accept" -> "*/*",
      "origin" -> "https://www.ebay.com",
      "accept-language" -> "zh-CN,zh;q=0.8")

    println(url)

    val result = getResponse(url, header)
    println(result.length)  

    var title = "Item Title: " //reg
    var tmin = result.indexOf(title) + title.length + "</b>".length
    var tmax = 0
    var tout = tmin

    for(tout <- tmin to tmin + 999 if tmax==0) {
      if(result(tout) == '<') { 
        tmax = tout 
      }
      //print(":"+ tout +":")
    }
    println(tmin + ":" + tmax)
    println(result.substring(tmin, tmax))

    /* 2. */
    var qmin = result.indexOf("Date of Purchase")
    var qmax = result.indexOf("""new EbayConfig("BidHistory.Image")""")
    println(qmin +"#"+ qmax)
    
    val temp = result.substring(qmin, qmax)
    val data = temp.substring(temp.indexOf("""alt=" ">"""), temp.indexOf("</table>")).replaceAll("</td>", "")
    //println(data)
    
    val sb = StringBuilder.newBuilder
    var sp = data.split("</tr>")
    for(ss <- sp) {
      //println("---- "+ss)
        //i <- (0 until ss.length).reverse
        var tmax = 0; var tmin = 0
        for(i <- 0 to ss.length-1) {
          if(ss(i)=='>') tmin = i
          if(ss(i)=='<') tmax = i      

          if(tmax >tmin+1) {
            print(" #"+ (tmax - tmin) +":")
            print(ss.substring(tmin+1, tmax))
            tmin=0; tmax=0
          }
      }
      println(" ====")
    }

    for(s <- sp) {
      //println("--"+s)
      var ss = ""
      var in = 0
      var ou = 0
      var i = 0
      for (i <- 0 to s.length-1) {
        if(s(i)=='>') in = i
        if(s(i)=='<') ou = i
        if(ou > in+1 && in >0 && ou >0) {          
          ss = s.substring(in, ou)
          //print(ss)
          sb.append(ss)
          in = 0
          ou = 0
        }
      }
      if(ss.length > 0) {
        sb.append('\n')
      }
    }

    //println(sb)
  }

  def writefile(sp: Array[String]) {
    //val pattern = new Regex("href=(.)</a></li>")
    val out = new FileWriter("/Users/hank/Documents/temp/ebay.txt", true)
    //out.write(str)

    var st = ""
    for(s <- sp) {
      var s_ = s.substring(0, s.indexOf(":"))
      if(! st.equals(s_)) {
        st = s_
        println(s_)
        out.write(s_)
        out.write('\n') 
      }
    }

    out.close()
  }


  def main(args: Array[String]): Unit = {
    //val url = "https://www.ebay.com/b/Womens-Clothing/15724/bn_661783"
    //curl 'https://offer.ebay.com/ws/eBayISAPI.dll?ViewBidsLogin&item=132179024122&rt=nc&_trksid=p2047675.l2564' -o 132179024122.html
    
    /* 1. */
    val url = "https://cn.ebay.com/b/Womens-Accessories/4251/bn_1519247?rt=nc&_pgn=1"
    //val data = getData(url)

    /* 2. */
    val dll = "https://offer.ebay.com/ws/eBayISAPI.dll?ViewBidsLogin&item=274434388200&rt=nc"
    getDll(dll)

    //hdfs()
  }

}