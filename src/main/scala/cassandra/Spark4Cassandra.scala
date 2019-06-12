package cassandra

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Kerven on 2019/6/12.
  *
  * 　　　　　　　   ┏┓　   ┏┓+ +
  * 　　　　　　　┏┛┻━━━┛┻┓ + +
  * 　　　　　　　┃　　　　　　　┃
  * 　　　　　　　┃　　　━　　　┃ ++ + + +
  * 　　　　　　 ████━████ ┃+
  * 　　　　　　　┃　　　　　　　┃ +
  * 　　　　　　　┃　　　┻　　　┃
  * 　　　　　　　┃　　　　　　　┃ + +
  * 　　　　　　　┗━┓　　　┏━┛
  * 　　　　　　　　　┃　　　┃
  * 　　　　　　　　　┃　　　┃ + + + +
  * 　　　　　　　　　┃　　　┃			
  * 　　　　　　　　　┃　　　┃ +			
  * 　　　　　　　　　┃　　　┃
  * 　　　　　　　　　┃　　　┃　　+
  * 　　　　　　　　　┃　 　　┗━━━┓ + +
  * 　　　　　　　　　┃ 　　　　　　　┣┓
  * 　　　　　　　　　┃ 　　　　　　　┏┛
  * 　　　　　　　　　┗┓┓┏━┳┓┏┛ + + + +
  * 　　　　　　　　　　┃┫┫　┃┫┫
  * 　　　　　　　　　　┗┻┛　┗┻┛+ + + +
  *
  * Don't bibi , show me the code
  *
  * spark write data to cassandra
  *
  */
object Spark4Cassandra {

  // basic configration
  val sparkConf = new SparkConf()
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.rdd.compress", "true")
  sparkConf.set("hive.metastore.warehouse.dir", "hdfs://xxx")
  sparkConf.set("spark.sql.warehouse.dir", "hdfs://XXX")
  sparkConf.set("spark.cassandra.connection.host", "your cassandra host ip")
  sparkConf.set("spark.cassandra.auth.username", "username")
  sparkConf.set("spark.cassandra.auth.password", "password")
  sparkConf.set("spark.cassandra.output.concurrent.writes", "50")
  //    sparkConf.set("spark.cassandra.output.batch.size.rows", "1")
  val spark = SparkSession.builder()
    .config(sparkConf)
    .appName(this.getClass.getSimpleName).enableHiveSupport()
    .master("local[*]")
    .getOrCreate()


  // datasource
  val loadDF = spark.sql(
    s"""
       |select
       |      mac as mac,
       |      did as uid,
       |      "aa" as did
       |from dmp_db.base_user_tags_v3
       |where  day='2019-05-21'
       |and mac <> ''

      """.stripMargin)

  loadDF.show(10)
  println("/////////////////////////")

  loadDF.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "coocaa", "table" -> "video_tag")).mode(SaveMode.Append).save

  spark.stop()


}
