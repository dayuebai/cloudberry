package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.{IQLGenerator, IQLGeneratorFactory, QueryParsingException}
import edu.uci.ics.cloudberry.zion.model.schema._
import org.joda.time.DateTime

import play.api.libs.json._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Defines constant strings for query languages supported by AsterixDB
  */
trait ElasticImpl {

  val aggregateFuncMap: Map[AggregateFunc, String]

  def getAggregateStr(aggregate: AggregateFunc): String = {
    aggregateFuncMap.get(aggregate) match {
      case Some(impl) => impl
      case None => throw new QueryParsingException(s"No implementation is provided for aggregate function ${aggregate.name}")
    }
  }
}

object ElasticImpl extends ElasticImpl {
  val aggregateFuncMap: Map[AggregateFunc, String] = Map(
    Count -> "count",
    Max -> "max",
    Min -> "min",
    Avg -> "avg",
    Sum -> "sum"
    // implement distinct_count, topK later
  )
}


class ElasticsearchGenerator extends IQLGenerator {

  /**
    * represent the expression for a [[Field]]
    *
    * @param refExpr the expression for referring this field by the subsequent statements
    * @param defExpr the expression the defines the field
    *
    */
  case class FieldExpr(refExpr: String, defExpr: String)

  /**
    * Partial parse results after parsing each [[Statement]]
    *
    * @param strs    a sequence of parsed query strings, which would be composed together later.
    * @param exprMap a new field expression map
    */
  case class ParsedResult(strs: Seq[String], exprMap: Map[String, FieldExpr])

  protected val allFieldVar: String = "*"

  // Global variable: JOIN field
  protected var joinTermsFilter = Seq[Int]()

  protected val typeImpl: ElasticImpl = ElasticImpl

  def generate(query: IQuery, schemaMap: Map[String, AbstractSchema]): String = {
    println("Start to generate query: " + query);
    val (temporalSchemaMap, lookupSchemaMap) = GeneratorUtil.splitSchemaMap(schemaMap)
    val result = query match {
      case q: Query => parseQuery(q, temporalSchemaMap)
      case q: CreateView => parseCreate(q, temporalSchemaMap)
//      case q: AppendView => parseAppend(q, temporalSchemaMap)
      case q: UpsertRecord => parseUpsert(q, schemaMap)
//      case q: DropView => parseDrop(q, schemaMap)
//      case q: DeleteRecord => parseDelete(q, schemaMap)
      case _ => ???
    }
    println("Translated to: " + result)
    result
  }

  def calcResultSchema(query: Query, schema: Schema): Schema = {
    if (query.lookup.isEmpty && query.groups.isEmpty && query.select.isEmpty) {
      schema.copySchema
    } else {
      ???
    }
  }

  protected def genProperties(schema: AbstractSchema): JsObject = {
    var properties = Json.obj()
    // TODO: user.create has time format: strict_date (in Elasticsearch)
    schema.fieldMap.values.filter(f => f.dataType == DataType.Time).foreach(
      f => {properties += (f.name -> Json.parse("""{ "type" : "date", "format": "strict_date_time" }"""))}
    )
    println("properties: " + properties)
    properties
  }

  def parseQuery(query: Query, schemaMap: Map[String, Schema]): String = {
    println("Call parseQuery")
    println("parseQuery query: " + query)
    var queryBuilder = Json.obj()

    val exprMap: Map[String, FieldExpr] = initExprMap(query.dataset, schemaMap)
//    println("SchemaMap: " + schemaMap)
//    println("ExprMap: " + exprMap)

//    println("dataset name: " + query.dataset)
    queryBuilder += ("method" -> JsString("search"))
    queryBuilder += ("dataset" -> JsString(query.dataset))

//    val (resultAfterLookup, queryAfterLookup) = parseLookup(query.lookup, exprMap, queryBuilder, false)

    val (resultAfterUnnest, queryAfterUnnest) = parseUnnest(query.unnest, exprMap, queryBuilder)
    val unnestTests = resultAfterUnnest.strs

    val (resultAfterFilter, queryAfterFilter) = parseFilter(query.filter, resultAfterUnnest.exprMap, unnestTests, queryAfterUnnest)

    val (resultAfterAppend, queryAfterAppend) = parseAppend(query.append, resultAfterFilter.exprMap, queryAfterFilter)

    val (resultAfterGroup, queryAfterGroup) = parseGroupby(query.groups, resultAfterAppend.exprMap, queryAfterAppend)

    val (resultAfterSelect, queryAfterSelect) = parseSelect(query.select, resultAfterGroup.exprMap, query, queryAfterGroup)

    val (resultAfterGlobalAggr, queryAfterGlobalAggr) = parseGlobalAggr(query.globalAggr, resultAfterSelect.exprMap, query, queryAfterSelect)

    queryAfterGlobalAggr.toString()

  }

  def parseCreate(create: CreateView, schemaMap: Map[String, Schema]): String = {
    println("Call parseCreate")
    val dataset = create.dataset
    val sourceSchema = schemaMap(create.query.dataset)
    val resultSchema = calcResultSchema(create.query, sourceSchema)
    val properties = genProperties(resultSchema)

    val dropStatement = Json.parse(
      s"""{ "method": "drop", "dataset": "$dataset" }"""
    )
    val createStatement = Json.parse(
      s"""{ "method": "create", "dataset": "$dataset", "mappings": { "_doc": { "properties": $properties } }, "settings": { "index": { "max_result_window": 2147483647 } } }"""
    )
    var selectStatement = Json.parse(parseQuery(create.query, schemaMap)).as[JsObject]

    var source = Json.obj()
    source += ("index" -> JsString(create.query.dataset))
    source += ("query" -> (selectStatement \ "query").get)
    val dest = Json.parse(s""" { "index": "$dataset" } """)

    var reindexStatement = Json.obj()
    reindexStatement += ("method" -> JsString("reindex"))
    reindexStatement += ("source" -> source)
    reindexStatement += ("dest" -> dest)

    val resQueryArray = Json.arr(dropStatement, createStatement, reindexStatement)
    println("resQueryArray: " + resQueryArray)
    resQueryArray.toString()
  }

  protected def parseAppend(append: AppendView, schemaMap: Map[String, Schema]): String = {
    println("Call parseAppend")
    println("append: " + append)
    return ""
  }

  def parseUpsert(q: UpsertRecord, schemaMap: Map[String, AbstractSchema]): String = {
    println("Call parseUpsert")
    var queryBuilder = Json.obj()
    var recordBuilder = Json.arr()
    val schema = schemaMap(q.dataset)
    val primaryKeyList = schema.getPrimaryKey

    for (record <- q.records.as[List[JsValue]]) {
      val idBuilder = new StringBuilder()
      var docBuilder = Json.obj()
      // Assume primary key is not a subfield of JSON data
      for (field <- primaryKeyList) {
        val id = (record \ (field.name)).get.toString()
        idBuilder.append(id)
      }
      // println("id: " + idBuilder.toString)
      recordBuilder = recordBuilder :+ Json.obj(("update" -> Json.obj("_id" -> Json.parse(idBuilder.toString))))
      docBuilder += ("doc" -> record)
      docBuilder += ("doc_as_upsert" -> JsBoolean(true))
      recordBuilder = recordBuilder :+ docBuilder
    }
    queryBuilder += ("method" -> JsString("upsert"))
    queryBuilder += ("dataset" -> JsString(q.dataset))
    queryBuilder += ("records" -> recordBuilder)
    queryBuilder.toString()
  }

//  protected def parseDrop(query: DropView, schemaMap: Map[String, AbstractSchema]): String = {
//    println("Call parseDrop")
//    return ""
//  }
//
//  protected def parseDelete(delete: DeleteRecord, schemaMap: Map[String, AbstractSchema]): String = {
//    println("Call parseDelete")
//    return ""
//  }
//
  // Private
  private def parseUnnest(unnest: Seq[UnnestStatement],
                          exprMap: Map[String, FieldExpr], queryAfterLookup: JsObject): (ParsedResult, JsObject) = {
    (ParsedResult(Seq.empty, exprMap), queryAfterLookup)
  }

  private def parseFilter(filters: Seq[FilterStatement], exprMap: Map[String, FieldExpr], unnestTestStrs: Seq[String], queryAfterUnnest: JsObject): (ParsedResult, JsObject) = {
    var shallowQueryAfterUnnest = queryAfterUnnest

    if (filters.isEmpty && unnestTestStrs.isEmpty) {
      (ParsedResult(Seq.empty, exprMap), shallowQueryAfterUnnest)
    } else {
      val filterStrs = filters.map { filter =>
        parseFilterRelation(filter, exprMap(filter.field.name).refExpr)
      }
      val filterStr = (unnestTestStrs ++ filterStrs).mkString("""{"must": [""", ",", "]}")

      shallowQueryAfterUnnest += ("query" -> Json.obj("bool" -> Json.parse(filterStr)))

      (ParsedResult(Seq.empty, exprMap), shallowQueryAfterUnnest)
    }
  }

  private def parseAppend(appends: Seq[AppendStatement], exprMap: Map[String, FieldExpr], queryAfterFilter: JsObject): (ParsedResult, JsObject) = {
    (ParsedResult(Seq.empty, exprMap), queryAfterFilter)
  }

  private def parseGroupby(groupOpt: Option[GroupStatement],
                           exprMap: Map[String, FieldExpr],
                           queryAfterAppend: JsObject): (ParsedResult, JsObject) = {
    var shallowQueryAfterAppend = queryAfterAppend
    groupOpt match {
      case Some(group) =>
        shallowQueryAfterAppend += ("size" -> JsNumber(0))
        val groupStr = new mutable.StringBuilder()
        var groupAsArray = Json.arr()

        for (i <- group.bys.indices) {
          val by = group.bys(i)
          val as = by.as.getOrElse(by.field).name
          groupAsArray = groupAsArray.append(JsString(as))
          if (i == 0)
            groupStr.append(s"""{"$as": {""")
          else
            groupStr.append(s""","aggs": {"$as": {""")

          by.funcOpt match {
            case Some(func) =>
              func match {
                case bin: Bin => ???
                case interval: Interval => // assume has time field
                  val duration = parseIntervalDuration(interval)
                  groupStr.append(s""" "date_histogram": {"field": "${by.field.name}", "interval": "$duration"} """.stripMargin)
                case level: Level =>
                  val hierarchyField = by.field.asInstanceOf[HierarchyField]
                  val field = hierarchyField.levels.find(_._1 == level.levelTag).get._2
                  if (group.lookups.isEmpty) {
                    groupStr.append(s""" "terms": {"field": "${field}", "size": 2147483647} """.stripMargin)
                  }
                  else { // hard coded sequence for later joins
                    groupStr.append(s""" "terms": {"field": "${field}", "size": 2147483647, "order": {"_key":"asc"}} """.stripMargin)
                  }
                case GeoCellTenth => ???
                case GeoCellTenth => ???
                case GeoCellHundredth => ???
                case _ => throw new QueryParsingException(s"unknown function: ${func.name}")
              }
            case None => ???
          }
        }
        groupStr.append("}" * group.bys.length * 2)
        shallowQueryAfterAppend += ("aggs" -> Json.parse(groupStr.toString))


        // Only support single join (Snowflake not supported)
        if (!group.lookups.isEmpty) {

          var queryArray = Json.arr()
          val selectDataset = (shallowQueryAfterAppend \ "dataset").get.as[JsString]
          shallowQueryAfterAppend -= "dataset"
          shallowQueryAfterAppend -= "method"
          queryArray = queryArray :+ Json.obj("index" -> selectDataset)
          queryArray = queryArray :+ shallowQueryAfterAppend

          val body = group.lookups.head
          val joinQuery = Json.parse(s"""{"_source": "${body.selectValues.head.name}", "size": 2147483647, "sort": {"${body.lookupKeys.head.name}": { "order": "asc" }}, "query": {"bool": {"must": {"terms": { "${body.lookupKeys.head.name}" : ${joinTermsFilter.mkString("[", ",", "]")} } } } } }""".stripMargin).as[JsObject]
          queryArray = queryArray :+ Json.obj("index" -> JsString(body.dataset.toLowerCase())) // The name of dataset in Elasticsearch must be in lowercase.
          queryArray = queryArray :+ joinQuery

          var multiSearchQuery = Json.obj("method" -> JsString("msearch"))
          multiSearchQuery += ("queries" -> queryArray)
          multiSearchQuery += ("groupAsList" -> groupAsArray)
          multiSearchQuery += ("joinSelectField" -> JsString(body.selectValues.head.name))
          multiSearchQuery += ("joinTermsFilter" -> Json.toJson(joinTermsFilter))

          println("multiSearchQuery: " + multiSearchQuery.toString())
          return (ParsedResult(Seq.empty, exprMap), multiSearchQuery)
        }

        shallowQueryAfterAppend += ("groupAsList" -> groupAsArray)
        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterAppend)

      case None =>
        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterAppend)
    }
  }

  private def parseSelect(selectOpt: Option[SelectStatement],
                          exprMap: Map[String, FieldExpr], query: Query,
                          queryAfterGroup: JsObject): (ParsedResult, JsObject) = {
    var shallowQueryAfterGroup = queryAfterGroup
    selectOpt match {
      case Some(select) =>
        val orderStrs = select.orderOn.zip(select.order).map {
          case (orderOn, order) =>
            val expr = orderOn.name
            val orderStr = if (order == SortOrder.DSC) "desc" else "asc"
            s"""|{"$expr": {"order": "$orderStr"}}""".stripMargin
        }
        val orderStr =
          if (!orderStrs.isEmpty) {
            orderStrs.mkString("[", ",", "]")
          } else {
            ""
          }
        val limit = select.limit
        val offset = select.offset
        println("limitStr: " + limit)
        println("offsetStr: " + offset)


        if (!orderStr.isEmpty())
          shallowQueryAfterGroup += ("sort" -> Json.parse(orderStr))
        if (limit != None)
          shallowQueryAfterGroup += ("size" -> JsNumber(limit))
        if (offset != None)
          shallowQueryAfterGroup += ("from" -> JsNumber(offset))

        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterGroup)
      case None =>
        println("in parseSelect function, case None is matched")
        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterGroup)
    }
  }

  private def parseGlobalAggr(globalAggrOpt: Option[GlobalAggregateStatement],
                              exprMap: Map[String, FieldExpr], query: Query,
                              queryAfterSelect: JsObject): (ParsedResult, JsObject) = {
    var shallowQueryAfterSelect = queryAfterSelect
    globalAggrOpt match {
      case Some(globalAggr) =>
        val aggr = globalAggr.aggregate
        val field = aggr.field.name
        val as = aggr.as.name
        val funcName = typeImpl.getAggregateStr(aggr.func)
        val aggregatedJson = Json.parse(s"""{"func": "$funcName", "as": "$as"}""")
        globalAggr.aggregate.func match {
          case Count => {
            shallowQueryAfterSelect += {"aggregation" -> aggregatedJson}
            if (field != "*") {
              shallowQueryAfterSelect += ("_source" -> Json.arr(JsString(field)))
            }
          }
          case Min | Max => {
            shallowQueryAfterSelect += ("aggregation" -> aggregatedJson)
            shallowQueryAfterSelect += ("size" -> JsNumber(0))
            shallowQueryAfterSelect += ("aggs" -> Json.obj( as -> Json.obj(funcName -> Json.obj("field" -> JsString(field)))))
          }
          // TODO
          case Sum => ???

          case Avg => ???

          case DistinctCount => ???

          case topK => ???
        }

        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterSelect)
      case None =>
        (ParsedResult(Seq.empty, exprMap), shallowQueryAfterSelect)
    }
    (ParsedResult(Seq.empty, exprMap), shallowQueryAfterSelect)
  }

  protected def initExprMap(dataset: String, schemaMap: Map[String, AbstractSchema]): Map[String, FieldExpr] = {
    // Type of val schema: Schema
    val schema = schemaMap(dataset)
    schema.fieldMap.mapValues { f =>
      f.dataType match {
        case DataType.Record => FieldExpr(allFieldVar, allFieldVar) // TODO: e.g. AllField, consider other cases
        case DataType.Hierarchy => FieldExpr(allFieldVar, allFieldVar) // TODO: to be implemented
        case _ => {
          FieldExpr(f.name, f.name)
        }
      }
    }
  }

  protected def parseFilterRelation(filter: FilterStatement, fieldExpr: String): String = {
    if ((filter.relation == Relation.isNull || filter.relation == Relation.isNotNull) &&
        filter.field.dataType != DataType.Bag && filter.field.dataType != DataType.Hierarchy) {
//      if (filter.relation == Relation.isNull)
//        s"$fieldExpr is unknown"
//      else
//        s"$fieldExpr is not unknown"
      ""
    }
    else {
      filter.field.dataType match {
        case DataType.Number =>
          parseNumberRelation(filter, fieldExpr)
        case DataType.Time =>
          parseTimeRelation(filter, fieldExpr)
        case DataType.Point => ???
//          parsePointRelation(filter, fieldExpr)
        case DataType.Boolean => ???
        case DataType.String => ???
//          parseStringRelation(filter, fieldExpr)
        case DataType.Text =>
          parseTextRelation(filter, fieldExpr)
        case DataType.Bag => ???
        case DataType.Hierarchy =>
          throw new QueryParsingException("the Hierarchy type doesn't support any relations.")
        case _ => throw new QueryParsingException(s"unknown datatype: ${
          filter.field.dataType
        }")
      }
    }
  }

  protected def parseNumberRelation(filter: FilterStatement, fieldExpr: String): String = {
    filter.relation match {
      case Relation.inRange =>
        if (filter.values.size != 2) throw new QueryParsingException(s"relation: ${filter.relation} requires two parameters")
        s"""{"range": {"$fieldExpr": {"gte": ${filter.values(0)}, "lt": ${filter.values(1)}}}}""".stripMargin
      case Relation.in => {
        joinTermsFilter = filter.values.asInstanceOf[Seq[Int]]
        s"""{"terms": {"$fieldExpr": [${filter.values.mkString(",")}]}}""".stripMargin
      }
      case Relation.< =>
        s"""{"range": {"$fieldExpr": {"lt": ${filter.values(0)}}}}""".stripMargin
      case Relation.> =>
        s"""{"range": {"$fieldExpr": {"gt": ${filter.values(0)}}}}""".stripMargin
      case Relation.<= =>
        s"""{"range": {"$fieldExpr": {"lte": ${filter.values(0)}}}}""".stripMargin
      case Relation.>= =>
        s"""{"range": {"$fieldExpr": {"gte": ${filter.values(0)}}}}""".stripMargin
      case _ => throw new QueryParsingException("no supported parameter for this number in Elasticsearch")
    }
  }

  protected def parseTimeRelation(filter: FilterStatement, fieldExpr: String): String = {
    filter.relation match {
      case Relation.inRange => {
        s"""{"range": {"$fieldExpr": {"gte": "${filter.values(0)}", "lt": "${filter.values(1)}", "format": "strict_date_time" }}}""".stripMargin
      }
      case Relation.< => {
        s"""{"range": {"$fieldExpr": {"lt": "${filter.values(0)}", "format": "strict_date_time" }}}""".stripMargin
      }
      case Relation.> => {
        s"""{"range": {"$fieldExpr": {"gt": "${filter.values(0)}", "format": "strict_date_time" }}}""".stripMargin
      }
      case Relation.<= => {
        s"""{"range": {"$fieldExpr": {"lte": "${filter.values(0)}", "format": "strict_date_time" }}}""".stripMargin
      }
      case Relation.>= => {
        s"""{"range": {"$fieldExpr": {"gte": "${filter.values(0)}", "format": "strict_date_time" }}}""".stripMargin
      }
      case _ => throw new QueryParsingException("no supported parameter for this date in Elasticsearch")
    }
  }

  protected def parsePointRelation(filter: FilterStatement, fieldExpr: String): String = {
    ""
  }

  protected def parseStringRelation(filter: FilterStatement, fieldExpr: String): String = {
    ""
  }

  protected def parseTextRelation(filter: FilterStatement, fieldExpr: String): String = {
    val words = filter.values.map(w => s"${w.asInstanceOf[String].trim()}").mkString("\"", ",", "\"")
    s"""{"match": {"${fieldExpr}": {"query": $words, "operator": "and"}}}"""
  }

  protected def parseIntervalDuration(interval: Interval): String = {
    import TimeUnit._
    interval.unit match {
      case Second => "second"
      case Minute => "minute"
      case Hour => "hour"
      case Day => "day"
      case Week => "week"
      case Month => "month"
      case Year => "year"
    }
  }
}

object ElasticsearchGenerator extends IQLGeneratorFactory {
  override def apply(): IQLGenerator = new ElasticsearchGenerator()
}