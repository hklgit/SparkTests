package kafka


import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.AccumulatorMetadata
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
  * Created by hankl on 2018/7/19.
  */
object KafkaTest {


  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)
    val conf 
//    conf.setMaster("local[*]")
    conf.setAppName(s"${this.getClass.getSimpleName}")
//    val sc: SparkContext = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)

    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = session.sparkContext
    val rdd: RDD[Row] = session.sql("select mac from dmp_db.base_user_tags where partition_day='2018-07-18' limit 1000").rdd

    var counter = sc.longAccumulator("kafkaCounter")

    /***
      *  ----------- for local test      add by hkl  2018-07-27
      *  val arr = Array("1ab","2ab","3ab","4ab","5ab","6ab","7ab","8ab","9ab","11a","22ab","33ab","44ab","55ab","66ab","77ab","88ab","99ab")
      *  val rdd = sc.parallelize(arr)
      *
      */

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
//        p.setProperty("bootstrap.servers", "192.168.0.19:6667")
        p.setProperty("bootstrap.servers", "192.168.2.140:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      logger.info("kafkaSink  init done !")
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //输出到kafka
    val start_time = System.currentTimeMillis();
    rdd.map(row=>row.getAs[String]("mac")).foreach(record=>{
      counter.add(1L)
      kafkaProducer.value.send("hkl719test", record)
    })

    val end_time = System.currentTimeMillis();

    val dur = end_time - start_time

    logger.info("==========消耗时常===========" + dur)
    logger.info("==========总记发送的消息条数===========" + counter)





  }

}
