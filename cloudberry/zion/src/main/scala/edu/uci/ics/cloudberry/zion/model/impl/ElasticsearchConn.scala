package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import play.api.Logger
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class ElasticsearchConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn {

  import ElasticsearchConn._

  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  def postQuery(query: String): Future[JsValue] = {
    postWithCheckingStatus(query, (ws: WSResponse) => {
      ws.json.asInstanceOf[JsObject].value("results")
    }, (ws: WSResponse) => defaultQueryResponse)
  }

  def postControl(query: String): Future[Boolean] = {
    postWithCheckingStatus(query, (ws: WSResponse) => true, (ws: WSResponse) => false)
  }

  protected def postWithCheckingStatus[T](query: String, succeedHandler: WSResponse => T, failureHandler: WSResponse => T): Future[T] = {
    post(query).map { wsResponse =>
      if (wsResponse.status == 200) {
        println("Query succeeded")
        Logger.info("Query succeeded: " + Json.prettyPrint(wsResponse.json))
        succeedHandler(wsResponse)
      }
      else {
        Logger.error("Query failed: " + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
  }

  def post(query: String): Future[WSResponse] = {
    Logger.debug("Query:" + query)
    val f = wSClient.url(url + "/berry.meta").withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).put(query)
    f.onFailure(wsFailureHandler(query))
    f
  }

  protected def wsFailureHandler(query: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => Logger.error("WS ERROR:" + query, e)
      throw e
  }

//  protected def params(query: String): Map[String, Seq[String]] = {
//
//  }
}

object ElasticsearchConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}

