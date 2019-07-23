package es

import com.coocaa.common.DateUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, TaskContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
/**
  * Created by Kerven on 2019/1/15.
  * spark write to es
  */


object UserTagsDid2ES {

  case class Tags(did: String, tc_version: String, chip: String, city: String, event_tags: String, content_tags: String, source: String, size: String, model: String, app_version: String, pay_tags: String, user_tags: String, province: String)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
   /* //获取conf传入到appname
    val name = conf.get("spark.app.name")
    name match {
      case null => conf.setAppName(name)
      case _ => println("appname=" + name)
    }*/
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //设置任务并发数
    conf.set("spark.sql.parquet.compression.codec", "snappy")
//    conf.set("hive.metastore.uris", "thrift://xxx.com:9083")
    conf.set("es.nodes", "xxxx,xxx,xxxx")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    conf.set("es.batch.size.entries", "5000")
    conf.set(ConfigurationOptions.ES_BATCH_WRITE_REFRESH, "false")
    conf.set("spark.sql.warehouse.dir", "hdfs://xxxxx/apps/hive/warehouse")
    conf.setMaster("local[*]")


    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val date = conf.get("spark.input.date", DateUtil.getDateBeforeDay)
    val dateStr = DateUtil.getFormatDate(DateUtil.getDateFromString(date, "yyyy-MM-dd"), "yyyy-MM-dd")


    TaskContext.get().taskMetrics().outputMetrics.recordsWritten

    spark.sparkContext.doubleAccumulator


    import spark.implicits._
    import spark.sql

    val mediaName = "test15/tags"
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val data = spark.sql(s""" select did,tc_version,chip,city,event_tags,content_tags,source,size,model,app_version,pay_tags,user_tags,province from  temp.dmp_did_tags """.stripMargin).repartition(10)
    val dd = data.map { row =>
      val did = row.getAs[String]("did")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val tc_version = row.getAs[String]("tc_version")
      val app_version = row.getAs[String]("app_version")
      val model = row.getAs[String]("model")
      val chip = row.getAs[String]("chip")
      val size = row.getAs[String]("size")
      val source = row.getAs[String]("source")
      val event_tags = row.getAs[String]("event_tags")
      val user_tags = row.getAs[String]("user_tags")
      val pay_tags = row.getAs[String]("pay_tags")
      val content_tags = row.getAs[String]("content_tags")




       Tags(did, tc_version, chip, city, event_tags, content_tags, source, size, model, app_version, pay_tags, user_tags, province)


    }

    dd.rdd.count()
    EsSpark.saveToEs(dd.rdd, mediaName, Map("es.mapping.id" -> "did"))
    spark.stop()


  }
}
