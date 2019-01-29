package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import play.api.Logger
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class ElasticsearchConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn {

  import ElasticsearchConn._

  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  def postQuery(query: String): Future[JsValue] = {
    postWithCheckingStatus(query, (ws: WSResponse, query) => {parseResponse(ws.json, query)}, (ws: WSResponse) => defaultQueryResponse)
  }

  def postControl(query: String): Future[Boolean] = {
    postWithCheckingStatus(query, (ws: WSResponse, query) => true, (ws: WSResponse) => false)
  }

  protected def postWithCheckingStatus[T](query: String, succeedHandler: (WSResponse, String) => T, failureHandler: WSResponse => T): Future[T] = {
    post(query).map { wsResponse =>
      if (wsResponse.status == 200) {
        println("Query succeeded")
        // Logger.info("Query succeeded: " + Json.prettyPrint(wsResponse.json))
        succeedHandler(wsResponse, query)
      }
      else {
        Logger.info("Query failed: " + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
  }

  def post(query: String): Future[WSResponse] = {
    var jsonQuery = Json.parse(query).as[JsObject]
    val method = (jsonQuery \ "method").get.toString().stripPrefix("\"").stripSuffix("\"")
    val dataset = (jsonQuery \ "dataset").get.toString().stripPrefix("\"").stripSuffix("\"")
    val jsonAggregation = (jsonQuery \ "aggregation").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""
    val queryURL = url + "/" + dataset
    val filterPath = aggregation match {
      case "" => "?filter_path=hits.hits._source"
      case "min" => "?size=0&filter_path=aggregations.min.value_as_string"
      case "max" => "?size=0&filter_path=aggregations.max.value_as_string"
      case _ => ???
    }

    jsonQuery -= "method"
    jsonQuery -= "dataset"
    jsonQuery -= "aggregation"

    Logger.info("Query: " + query)
    Logger.info("method: " + method)
    Logger.info("dataset: " + dataset)
    Logger.info("aggregation: " + aggregation)
    Logger.info("jsonQuery: " + jsonQuery.toString())

    val f = method match {
      case "create" => wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).put(jsonQuery)
      case "search" => wSClient.url(queryURL + "/_search" + filterPath).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(jsonQuery)
      case "delete" => ???
      case "upsert" => {
        val records = (jsonQuery \ "records").get.as[List[JsValue]].mkString("", "\n", "\n")
        wSClient.url(queryURL + "/_doc" + "/_bulk").withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(records)
      }
      case _ => {println("NO MATCH METHOD"); ???}
    }
    f.onFailure(wsFailureHandler(query))
    f
  }

  protected def wsFailureHandler(query: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => Logger.error("WS ERROR:" + query, e)
      throw e
  }

  protected def parseResponse(response: JsValue, query: String): JsValue = {
    var jsonQuery = Json.parse(query).as[JsObject]
    val jsonAggregation = (jsonQuery \ "aggregation").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""
    aggregation match {
      case "" => {
        val sourceJsValue = (response.asInstanceOf[JsObject] \ "hits" \ "hits").getOrElse(JsNull)
        if (sourceJsValue != JsNull) {
          val sourceArray = sourceJsValue.as[Seq[JsObject]]
          //      println("response: " + response)
          //      println("sourceArray: " + sourceArray)

          val returnArray = sourceArray.map(doc => doc.value("_source"))
          Json.toJson(returnArray)
        }
        Json.arr()
      }
      case "min" => {
        val min = (response.asInstanceOf[JsObject] \ "aggregations" \ "min" \ "value_as_string").get.as[JsString]
        val jsonObjMin = Json.obj(("min" -> min))
//        println("min reuturn: " + Json.arr(jsonObjMin))
        Json.arr(jsonObjMin)
      }
      case "max" => {
        val max = (response.asInstanceOf[JsObject] \ "aggregations" \ "max" \ "value_as_string").get.as[JsString]
        val jsonObjMax = Json.obj(("max" -> max))
//        println("max return: " + Json.arr(jsonObjMax))
        Json.arr(jsonObjMax)
      }
      case _ => ???
    }
  }
}

object ElasticsearchConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}
