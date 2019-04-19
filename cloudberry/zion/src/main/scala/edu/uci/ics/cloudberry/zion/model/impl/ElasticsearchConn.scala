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

  private def postWithCheckingStatus[T](query: String, succeedHandler: (WSResponse, String) => T, failureHandler: WSResponse => T): Future[T] = {
    post(query).map { wsResponse =>
      val responseCode = wsResponse.status
      if (responseCode == 200 || responseCode == 400) { // Create existing table results in status code: 400
        succeedHandler(wsResponse, query)
      }
      else {
        Logger.info("Query failed: " + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
  }

 private def transactionWithCheckingStatus(query: String, succeedHandler: (WSResponse) => Boolean, failureHandler: (WSResponse) => Boolean): Future[Boolean] = {
//    println("CALL TRANSACTION POST")
    var jsonQuery = Json.parse(query).as[Seq[JsObject]]
//    println("jsonQuery: " + jsonQuery)

    while (jsonQuery.length != 1) {
      val headQuery = jsonQuery.head.toString()
      jsonQuery = jsonQuery.drop(1)
//      println("headQuery: " + headQuery)
//      println("after drop, jsonquery is: " + jsonQuery)

      post(headQuery).map { wsResponse =>
        val responseCode = wsResponse.status
//        println("multi post Query, status code: " + responseCode + " query: " + headQuery)
      }
    }

    post(jsonQuery.head.toString()).map { wsResponse =>
      val responseCode = wsResponse.status
      if (responseCode == 200) {
//        Logger.info("FINISH TRANSACTION")
        succeedHandler(wsResponse)
      }
      else{
//        Logger.info("TRANSACTION POST QUERY FAILED: " + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
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
//          println(s"""?size=0&filter_path=aggregations.$asField.value_as_string""")
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

//    Logger.info("Query: " + query)
//    Logger.info("method: " + method)
//    Logger.info("dataset: " + dataset)
//    Logger.info("aggregation: " + aggregation)
//    Logger.info("jsonQuery: " + jsonQuery.toString())

    val f = method match {
      case "create" => wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).put(jsonQuery)
      case "search" => wSClient.url(queryURL + "/_search" + filterPath).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(jsonQuery)
      case "msearch" => {
        val queries = (jsonQuery \ "queries").get.as[List[JsValue]].mkString("", "\n", "\n") // Queries must be terminated by a new line character
//        println("QUERIES: " + queries)
        wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(queries)
      }
      case "upsert" => {
        val records = (jsonQuery \ "records").get.as[List[JsValue]].mkString("", "\n", "\n") // Queries must be terminated by a new line character
        wSClient.url(queryURL + "/_doc" + "/_bulk?refresh").withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(records)
      }
      case "drop" => wSClient.url(queryURL).withRequestTimeout(Duration.Inf).delete()
      case "reindex" => wSClient.url(queryURL).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf).post(jsonQuery)
      case _ => ???
    }
    f.onFailure(wsFailureHandler(query))
    f
  }

  private def wsFailureHandler(query: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => Logger.error("WS ERROR:" + query, e)
      throw e
  }

  private def parseResponse(response: JsValue, query: String): JsValue = {
    val jsonQuery = Json.parse(query).as[JsObject]
    val jsonAggregation = (jsonQuery \ "aggregation" \ "func").getOrElse(JsNull)
    val aggregation = if (jsonAggregation != JsNull) jsonAggregation.toString().stripPrefix("\"").stripSuffix("\"") else ""
    val jsonGroupAsList = (jsonQuery \ "groupAsList").getOrElse(JsNull)
    val joinSelectField = (jsonQuery \ "joinSelectField").getOrElse(JsNull)
//    println("json group as list: " + jsonGroupAsList)
    if (jsonGroupAsList != JsNull) {
//      println("jsongroupaslist EXISTS")
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
        for (bucket <- buckets) { // TODO: Refactor loop structure
          val keyValue = if (bucket.keys.contains("key_as_string")) (bucket \ "key_as_string").get else (bucket \ "key").get
          val jsLiquid = (bucket \ groupAsList.last \ "buckets").getOrElse(JsNull)
          if (jsLiquid != JsNull) {
            val liquid = jsLiquid.as[Seq[JsObject]]
            for (drop <- liquid) {
              var tmp_json = Json.obj()
              tmp_json += (groupAsList.head -> keyValue)
              tmp_json += (groupAsList.last -> JsString((drop \ "key_as_string").get.as[String]))
              tmp_json += ("count" -> JsNumber((drop \ "doc_count").get.as[Int]))
              resArray = resArray.append(tmp_json)
            }
          }
          else {
            var tmp_json = Json.obj()
            tmp_json += (groupAsList.head -> keyValue)
            tmp_json += ("count" -> JsNumber((bucket \ "doc_count").get.as[Int]))
            resArray = resArray.append(tmp_json)
          }
        }
      }
//      println("resArray is: " + resArray)
      return resArray
    }

    aggregation match {
      case "" => { // Search query without aggregation
        val sourceJsValue = (response.asInstanceOf[JsObject] \ "hits" \ "hits").getOrElse(JsNull)
        if (sourceJsValue != JsNull) {
          val sourceArray = sourceJsValue.as[Seq[JsObject]]
          //      println("response: " + response)
          //      println("sourceArray: " + sourceArray)
          if (jsonQuery.keys.contains("_source")) { // select bounding box / coordinate query will contains _source
            val returnArray = sourceArray.map(doc => parseSource(doc.value("_source").as[JsObject]))
//            println("HEATMAP/PINMAP RETURNS: " +  Json.toJson(returnArray))
            return Json.toJson(returnArray)
          }

          val returnArray = sourceArray.map(doc => doc.value("_source"))
          Json.toJson(returnArray)
        }
        Json.arr()
      }
      case "count" => { // Aggregation (function: count)
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
        val count = (response.asInstanceOf[JsObject] \ "hits" \ "total").get.as[JsNumber]
//        println(Json.arr(Json.obj(asField -> count)))
        Json.arr(Json.obj(asField -> count))
      }
      case "min" | "max" => { // Aggregation (function: min/max)
//        println("min/max response: " + response)
        val asField = (jsonQuery \ "aggregation" \ "as").get.toString().stripPrefix("\"").stripSuffix("\"")
//        if (bucket.keys.contains("key_as_string")) (bucket \ "key_as_string").get else (bucket \ "key").get

        if (response.as[JsObject].keys.nonEmpty) {
          val min_obj = (response \ "aggregations" \ asField).as[JsObject]
          val res = if (min_obj.keys.contains("value_as_string")) (min_obj \ "value_as_string").getOrElse(JsNull) else (min_obj \ "value").get

          if (res != JsNull) {
            val jsonObjRes = Json.obj(asField -> res)
//            println(s"$asField return: " + Json.arr(jsonObjRes))
            return Json.arr(jsonObjRes)
          }
        }

        // TODO: how to handle empty response in aggregation?
//          [error] e.u.i.c.z.a.DataStoreManager - collectStats error: java.util.NoSuchElementException: head of empty list

//        println(s"$asField return: " + Json.arr(Json.obj(asField -> JsString(""))))
//        Json.arr(Json.obj(asField -> JsString("")))
//        println(s"$asField return: " + Json.arr())
        Json.arr()
      }
      case _ => ??? // Unmatched
    }
  }

  // Helper function: find the index of an element in an array in O(logN) time.
  private def binarySearch(arr: Seq[Int], x: Int): Int = {
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

  // parse "_source" field in Elasticsearch response for heat map and pin map
  private def parseSource(source: JsObject): JsValue = {
    var returnSource = Json.obj()
    source.keys.foreach(key => {
      var curKey = key
      var value = (source \ curKey).get
      while (value.isInstanceOf[JsObject]) {
        val tmp = value.as[JsObject]
        curKey += "." + tmp.keys.head
        value = tmp.values.head
      }
      val valueString = value.toString().stripPrefix("\"").stripSuffix("\"")
      if (valueString.startsWith("point")) {
        val arrString = "[" + valueString.stripPrefix("point(").stripSuffix(")").replace(" ", ",") + "]"
        returnSource += (curKey -> Json.parse(arrString))
      }
      else if (valueString.startsWith("LINESTRING")) {
        val arrString = "[[" + valueString.stripPrefix("LINESTRING(").stripSuffix(")").replace(",", "],[").replace(" ", ",") + "]]"
        returnSource += (curKey -> Json.parse(arrString))
      }
      else{
        returnSource += (curKey -> value)
      }
    })
    returnSource
  }
}

object ElasticsearchConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}
