import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer

object Boot extends MultipartFormDataHandler {

  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  def main99(args : Array[String]) : Unit = {

	  Http().bindAndHandle(routes, "0.0.0.0", 9000) map { result =>
	    println("Server has started on port 9000...")
	  } recover {
	    case ex: Exception => println(s"Server binding failed due to ${ex.getMessage}")
	  }
  }

}
