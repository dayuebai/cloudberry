package db

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import edu.uci.ics.cloudberry.zion.model.impl._

import scala.concurrent.{ExecutionContext, Future}

private[db] class Migration_20160814() {

  import Migration_20160814._

  //TODO it supposes to automatically register the dataset from AsterixDB
  def up(conn: IDataConn)(implicit ec: ExecutionContext): Future[Boolean] = {
    conn match {
      case psql: PostgreSQLConn =>
        conn.postControl{
          s"""
             |create table if not exists "berry.meta" (
             |"name" varchar(255) not null,
             |"schema" json not null,
             |"dataInterval" json not null,
             |"stats" json not null,
             |"stats.createTime" time not null,
             |primary key("name")
             |);
           """.stripMargin
        }
      case sql: MySQLConn =>
        conn.postControl {
          s"""
             |create table if not exists `berry.meta` (
             |`name` varchar(255) not null,
             |`schema` json not null,
             |`dataInterval` json not null,
             |`stats` json not null,
             |`stats.createTime` datetime not null,
             |primary key(`name`)
             |)
             |""".stripMargin
        }
      case sqlpp: AsterixSQLPPConn =>
        conn.postControl {
          s"""
             |create dataverse berry if not exists;
             |create type berry.metaType if not exists as open {
             | name : string,
             | stats : { createTime: string}
             |};
             |
             |create dataset $berryMeta(berry.metaType) if not exists primary key name;
       """.stripMargin
        }
      case elasticsearch: ElasticsearchConn =>
        conn.postControl {
          s"""
             |{"mappings" : {
             |  "_doc" : {
             |    "properties" : {
             |      "dataInterval.start" : { "type" : "date", "format": "strict_date_time" },
             |      "dataInterval.end": { "type" : "date", "format": "strict_date_time" },
             |      "stats.createTime": { "type" : "date", "format": "strict_date_time" },
             |      "stats.lastModifyTime": { "type" : "date", "format": "strict_date_time" },
             |      "stats.lastReadTime": { "type" : "date", "format": "strict_date_time" }
             |    }
             |  }
             |}}
           """.stripMargin
        }
    }
  }

}

object Migration_20160814 {
  val migration = new Migration_20160814()
  val berryMeta = DataSetInfo.MetaDataDBName
}
