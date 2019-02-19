package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import play.api.Logger
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.Await

class ElasticsearchConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn {

  import ElasticsearchConn._

  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  def postQuery(query: String): Future[JsValue] = {
      postWithCheckingStatus(query, (ws: WSResponse, query) => {parseResponse(ws.json, query)}, (ws: WSResponse) => defaultQueryResponse)
  }

  def postControl(query: String): Future[Boolean] = {
    if (query.startsWith("[")) {
      transactionWithCheckingStatus(query, (ws: WSResponse) => true, (ws: WSResponse) => false)
    }
    else {
      postWithCheckingStatus(query, (ws: WSResponse, query) => true, (ws: WSResponse) => false)
    }
  }

  protected def postWithCheckingStatus[T](query: String, succeedHandler: (WSResponse, String) => T, failureHandler: WSResponse => T): Future[T] = {
    post(query).map { wsResponse =>
      val responseCode = wsResponse.status
      if (responseCode == 200 || responseCode == 400) {
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

  def transactionWithCheckingStatus(query: String, succeedHandler: (WSResponse) => Boolean, failureHandler: (WSResponse) => Boolean): Future[Boolean] = {
    println("CALL TRANSACTION POST")
    var jsonQuery = Json.parse(query).as[Seq[JsObject]]
    println("jsonQuery: " + jsonQuery)

    while (jsonQuery.length != 1) {
      val headQuery = jsonQuery.head.toString()
      jsonQuery = jsonQuery.drop(1)
      println("headQuery: " + headQuery)
      println("after drop, jsonquery is: " + jsonQuery)

      val c = post(headQuery).map { wsResponse =>
        val responseCode = wsResponse.status
        println("multi post Query, status code: " + responseCode + " query: " + headQuery)
      }
      Await.ready(c, Duration.Inf)
    }

    val r = post(jsonQuery.head.toString()).map { wsResponse =>
      val responseCode = wsResponse.status
      if (responseCode == 200) {
        Logger.info("FINISH TRANSACTION")
        succeedHandler(wsResponse)
      }
      else{
        Logger.info("TRANSACTION POST QUERY FAILED: " + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
    Await.ready(r, Duration.Inf)
  }

  def post(query: String): Future[WSResponse] = {
    var jsonQuery = Json.parse(query).as[JsObject]
    val method = (jsonQuery \ "method").get.toString().stripPrefix("\"").stripSuffix("\"")
    val jsonAggregation = (jsonQuery \ "aggregation" \ "func").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""
    var dataset = ""
    var queryURL = ""

    method match {
      case "reindex" =>  queryURL = url + "/_reindex?refresh"
      case "msearch" => queryURL = url + "/_msearch"
      case _ => {
        dataset = (jsonQuery \ "dataset").get.toString().stripPrefix("\"").stripSuffix("\"")
        queryURL = url + "/" + dataset
      }
    }

    var filterPath = ""
    if (method != "drop" && method != "create" && method != "msearch") {
      filterPath = aggregation match {
        case "" => if ((jsonQuery \ "groupAsList").getOrElse(JsNull) == JsNull) "?filter_path=hits.hits._source" else "?filter_path=aggregations"
        case "count" => "?filter_path=hits.total"
        case "min" | "max" => {
          val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
          println(s"""?size=0&filter_path=aggregations.$asField.value_as_string""")
          s"""?size=0&filter_path=aggregations.$asField.value_as_string"""
        }
        case _ => ???
      }
    }

    jsonQuery -= "method"
    jsonQuery -= "dataset"
    jsonQuery -= "aggregation"
    jsonQuery -= "groupAsList"
    jsonQuery -= "selectFields"
    jsonQuery -= "joinTermsFilter"

    Logger.info("Query: " + query)
    Logger.info("method: " + method)
    Logger.info("dataset: " + dataset)
    Logger.info("aggregation: " + aggregation)
    Logger.info("jsonQuery: " + jsonQuery.toString())

    val f = method match {
      case "create" => wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).put(jsonQuery)
      case "search" => wSClient.url(queryURL + "/_search" + filterPath).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(jsonQuery)
      case "msearch" => {
        val queries = (jsonQuery \ "queries").get.as[List[JsValue]].mkString("", "\n", "\n") // Queries must be terminated by a new line character
        println("QUERIES: " + queries)
        wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(queries)
      }
      case "upsert" => {
        val records = (jsonQuery \ "records").get.as[List[JsValue]].mkString("", "\n", "\n") // Queries must be terminated by a new line character
        wSClient.url(queryURL + "/_doc" + "/_bulk?refresh").withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(records)
      }
      case "drop" => wSClient.url(queryURL).withRequestTimeout(Duration.Inf).delete()
      case "reindex" => wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(jsonQuery)
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
    val joinSelectField = (jsonQuery \ "joinSelectField").getOrElse(JsNull)

    if (jsonGroupAsList != JsNull) {
      var resArray = Json.arr()
      val groupAsList = jsonGroupAsList.as[Seq[String]]

      if (joinSelectField != JsNull) { // JOIN query with aggregation
        val sortedJoinTermsFilter = (jsonQuery \ "joinTermsFilter").get.as[Seq[Int]].sorted
        val responseList= (response \ "responses").as[Seq[JsObject]]
        val buckets = (responseList.head \ "aggregations" \ groupAsList.head \ "buckets").get.as[Seq[JsObject]]
        val joinBucket = (responseList.last \ "hits" \ "hits").get.as[Seq[JsObject]]
        val joinSelectFieldString = joinSelectField.toString().stripPrefix("\"").stripSuffix("\"")

        for (bucket <- buckets) {
          val key = (bucket \ "key").get.as[Int]
          val count = (bucket \ "doc_count").get.as[Int]
          val keyIndex = binarySearch(sortedJoinTermsFilter, key)
          val joinValue = (joinBucket(keyIndex) \ "_source" \ joinSelectFieldString).get.as[Int]

          var tmp_json = Json.obj(groupAsList.head -> JsNumber(key))
          tmp_json += ("count" -> JsNumber(count))
          tmp_json += (joinSelectFieldString -> JsNumber(joinValue))
          resArray = resArray.append(tmp_json)
        }
      }
      else {
        val buckets: Seq[JsObject] = (response \ "aggregations" \ groupAsList.head \ "buckets").get.as[Seq[JsObject]]
        for (bucket <- buckets) {
          val keyValue = (bucket \ "key").get.as[Int]
          val liquid: Seq[JsObject] = (bucket \ groupAsList.last \ "buckets").get.as[Seq[JsObject]]
          for (drop <- liquid) {
            var tmp_json = Json.obj()
            tmp_json += (groupAsList.head -> JsNumber(keyValue))
            tmp_json += (groupAsList.last -> JsString((drop \ "key_as_string").get.as[String]))
            tmp_json += ("count" -> JsNumber((drop \ "doc_count").get.as[Int]))
            resArray = resArray.append(tmp_json)
          }
        }
      }
      println("resArray is: " + resArray)
      return resArray
    }

    aggregation match {
      case "" => { // Search query without aggregation
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
      case "count" => { // Aggregation (function: count)
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        val count = (response.asInstanceOf[JsObject] \ "hits" \ "total").get.as[JsNumber]
        println(Json.arr(Json.obj(asField -> count)))
        Json.arr(Json.obj(asField -> count))
      }
      case "min" | "max" => { // Aggregation (function: min/max)
        println("min/max response: " + response)
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        val res = (response.asInstanceOf[JsObject] \ "aggregations" \ asField \ "value_as_string").get.as[JsString]
        val jsonObjRes = Json.obj(asField -> res)
        println(s"$asField return: " + Json.arr(jsonObjRes))
        Json.arr(jsonObjRes)
      }
      case _ => ??? // Unmatched
    }
  }

  // Helper function: find the index of an element in an array in O(logN) time.
  protected def binarySearch(arr: Seq[Int], x: Int): Int = {
    var left = 0
    var right = arr.length - 1

    while (left <= right) {
      val middle = left + (right - left) / 2

      if (arr(middle) == x) {
        return middle
      } else if(arr(middle) < x) {
        left = middle + 1
      } else {
        right = middle - 1
      }
    }
    -1
  }
}

object ElasticsearchConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}
