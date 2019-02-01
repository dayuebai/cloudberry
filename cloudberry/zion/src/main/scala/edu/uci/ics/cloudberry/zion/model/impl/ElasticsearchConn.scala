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
    val jsonAggregation = (jsonQuery \ "aggregation" \ "func").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""

    val queryURL = url + "/" + dataset
    val filterPath = aggregation match {
      case "" => if ((jsonQuery \ "groupAsList").getOrElse(JsNull) == JsNull) "?filter_path=hits.hits._source" else "?filter_path=aggregations"
      case "count" => "?filter_path=hits.total"
      case "min" | "max" => {
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        println(s"""?size=0&filter_path=aggregations.$asField.value_as_string""")
        s"""?size=0&filter_path=aggregations.$asField.value_as_string"""
      }
      case _ => ???
    }

    jsonQuery -= "method"
    jsonQuery -= "dataset"
    jsonQuery -= "aggregation"
    jsonQuery -= "groupAsList"
    jsonQuery -= "join"

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
    val jsonQuery = Json.parse(query).as[JsObject]
    val jsonAggregation = (jsonQuery \ "aggregation" \ "func").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""
    val jsonGroupAsList = (jsonQuery \ "groupAsList").getOrElse(JsNull)

    if (jsonGroupAsList != JsNull) {
      var resArray = Json.arr()
      val groupAsList = jsonGroupAsList.as[Seq[String]]
      println("response for parseResponse" + response)
      println("group As List: " + groupAsList)
      val buckets: Seq[JsValue] = (response \ "aggregations" \ groupAsList.head \ "buckets").get.as[Seq[JsValue]]
      for (bucket <- buckets) {
        val keyValue = (bucket \ "key").get.as[Int]
        if ((jsonQuery \ "join").getOrElse(JsNull) == JsNull) {
          val liquid: Seq[JsValue] = (bucket \ groupAsList.last \ "buckets").get.as[Seq[JsValue]]
          for (drop <- liquid) {
            var tmp_json = Json.obj()
            tmp_json += (groupAsList.head -> JsNumber(keyValue))
            tmp_json += (groupAsList.last -> JsString((drop \ "key_as_string").get.as[String]))
            tmp_json += ("count" -> JsNumber((drop \ "doc_count").get.as[Int]))
            resArray = resArray.append(tmp_json)
          }
        }
        else {
          val docCount = (bucket \ "doc_count").get.as[Int]
          var jsonObj = Json.obj()
          jsonObj += (groupAsList.head -> JsNumber(keyValue))
          jsonObj += ("count" -> JsNumber(docCount))
          // Assume population is now TODO: implement join query
          jsonObj += ("population" -> JsNumber(1))
          resArray = resArray.append(jsonObj)
        }
      }
      println("retArray is: " + resArray)
      return resArray
    }

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
      case "count" => {
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        val count = (response.asInstanceOf[JsObject] \ "hits" \ "total").get.as[JsNumber]
        println(Json.arr(Json.obj(asField -> count)))
        Json.arr(Json.obj(asField -> count))
      }
      case "min" | "max" => {
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        val res = (response.asInstanceOf[JsObject] \ "aggregations" \ asField \ "value_as_string").get.as[JsString]
        val jsonObjRes = Json.obj(asField -> res)
        println(s"$asField return: " + Json.arr(jsonObjRes))
        Json.arr(jsonObjRes)
      }
      case _ => ???
    }
  }
}

object ElasticsearchConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}
