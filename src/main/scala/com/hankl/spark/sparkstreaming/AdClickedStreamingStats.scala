/*
package com.hankl.spark.sparkstreaming

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.LinkedBlockingQueue

import org.apache.avro.generic.GenericData.StringType
import org.apache.commons.codec.StringDecoder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._


object AdClickedStreamingStats {
  def main(args: Array[String]): Unit = {


    /**
     * 1,通过SparkConf设置应用程序的基本参数，和配置信息；
     * 2，通过SparkConf的setJars这个至关重要的方法可以声明程序运行依赖的jar，这是非常有价值，因为你可以程序运行需要的Jar都放在这里，例如说视频解析算法的Jar，例如so库文件等；
     * 3，在Spark 2.x版本中，可以通过SparkSession的conf方法把SparkConf对象设置进去；
     */
    val sparkConf = new SparkConf().setAppName("AdClickedStreamingStats")
       .setMaster("spark://192.168.189.1:7077").setJars( List(
     // .setMaster("local[5]").setJars(List(
        "/usr/local/spark-1.6.1-bin-hadoop2.6/lib/spark-streaming-kafka_2.10-1.6.1.jar",
        "/usr/local/kafka_2.10-0.8.2.1/libs/kafka-clients-0.8.2.1.jar",
        "/usr/local/kafka_2.10-0.8.2.1/libs/kafka_2.10-0.8.2.1.jar",
        "/usr/local/spark-1.6.1-bin-hadoop2.6/lib/spark-streaming_2.10-1.6.1.jar",
        "/usr/local/kafka_2.10-0.8.2.1/libs/metrics-core-2.2.0.jar",
        "/usr/local/kafka_2.10-0.8.2.1/libs/zkclient-0.3.jar",
      //  "/usr/local/spark-1.6.1-bin-hadoop2.6/lib/spark-assembly-1.6.1-hadoop2.6.0.jar",
        "/usr/local/spark-1.6.1-bin-hadoop2.6/lib/mysql-connector-java-5.1.13-bin.jar",
        "/usr/local/IMF_testdata/AdClickedStreamingStats.jar"))

    /**
     * 1, SparkStreaming本质上是微型的批处理模型，且基于时间间隔的方式来区分不同批次的Job；
     * 2，在SparkStreaming运行的时候通过监控台观测必须让每个Batch的处理时间小雨Batch Interval，如果不是这样的话，则需要进行调优，否则程序会崩溃；
     * 3，SparkStreaming特别适合延迟超过1分钟的流式在线处理；
     */
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    /**
     * 1,Driver高可用性如何实现？
     * 2，RDD的高可用性如何实现？
     * 3，数据的高可用性如何实现？
     */
    ssc.checkpoint("/usr/local/spark/AdClickedStreamingStats")

    //通过Direct API的方式直接连上Kafka集群，自己维护消费的数据（可以重复消费）
    val kafkaParameters = Map[String, String]("metadata.broker.list" -> "Master:9092,Worker1:9092,Worker2:9092")
    //Kafka的Consumers是分组的，我们写程序的时候必须设置组名，这样做的一个非常大的好处是例如说不同的公司的部分可以消费同样数据，但是进行不同类型的业务操作
    val topics = Set[String]("AdClickedStreamingStats")
    val adClickedStreaming = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters, topics)

    val filteredadClickedStreaming = adClickedStreaming.transform(rdd => {  //这里做了一个很长的转换操作，得到我们要的过滤黑名单的数据集
      val blackListNames = ListBuffer[String]()
      val  jdbcWrapper  =  JDBCWrapper.getInstance()

      def querycallBack(result: ResultSet): Unit = {

        while (result.next()) {
          result.getString(1)
          blackListNames += result.getString(1)
        }
      }

      jdbcWrapper.doQuery("SELECT * FROM blacklisttable", null, querycallBack)

      val blackListTuple = ListBuffer[(String, Boolean)]()
      for (name <- blackListNames) {
        val nameBoolean = (name, true)
        blackListTuple += nameBoolean
      }

      val blackListFromDB = blackListTuple

      /** The SparkContext that created this RDD. */
    val jsc =  rdd.sparkContext   // todo 这个我还是真没用过呢 hankl 2018-04-25

    //  val jsc = rdd.sparkContext
//new SparkContext()

     // val jsc = new SparkContext(rdd.sparkContext)

      val blackListRDD = jsc.parallelize(blackListFromDB)  //吧list转为rdd

      val rdd2Pair = rdd.map(t => {
        val userID = t._2.split("\t")(2)
        (userID, t)

      })

      val joined = rdd2Pair.leftOuterJoin(blackListRDD) // todo 这里用的是左外链接来操作的
      val result = joined.filter(v1 => {
        val optional = v1._2._2;
        if (optional.isDefined && optional.get) {  //这个代表的是都匹配上了，这部分是要去除的  这里也可以直接用isEmpty
          false
        } else {
          true
        }

      }).map(_._2._1) //test

      result

    })

    filteredadClickedStreaming.print()

		/* * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark
		 * Streaming具体 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
		 * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
		 */

    val pairs = filteredadClickedStreaming.map(t => {
      val splited = t._2.split("\t")
      val timestamp = splited(0) // yyyy-MM-dd
      val ip = splited(1)
      val userID = splited(2)
      val adID = splited(3)
      val province = splited(4)
      val city = splited(5)
      val clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_" + city
      (clickedRecord, 1L)
    })


		 /** 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 计算每个Batch Duration中每个User的广告点击量*/

    val adClickedUsers = pairs.reduceByKey(_ + _)

    /**
     *
     * 计算出什么叫有效的点击？ 1，复杂化的一般都是采用机器学习训练好模型直接在线进行过滤； 2，简单的？可以通过一个Batch
     * Duration中的点击次数来判断是不是非法广告点击，但是实际上讲非法广告
     * 点击程序会尽可能模拟真实的广告点击行为，所以通过一个Batch来判断是 不完整的，我们需要对例如一天（也可以是每一个小时）
     * 的数据进行判断！ 3，比在线机器学习退而求次的做法如下： 例如：一段时间内，同一个IP（MAC地址）有多个用户的帐号访问；
     * 例如：可以统一一天内一个用户点击广告的次数，如果一天点击同样的广告操作50次的话，就列入黑名单；
     *
     * 黑名单有一个重点的特征：动态生成！！！所以每一个Batch Duration都要考虑是否有新的黑名单加入，此时黑名单需要存储起来
     * 具体存储在什么地方呢，存储在DB/Redis中即可；
     *
     * 例如邮件系统中的“黑名单”，可以采用Spark Streaming不断的监控每个用户的操作，如果用户发送邮件的频率超过了设定的值，可以
     * 暂时把用户列入“黑名单”，从而阻止用户过度频繁的发送邮件。
     */

    val filteredClickInBatch = adClickedUsers.filter(v1 => {
      if (1 < v1._2) { // 更新一下黑名单的数据表
        false
      } else {
        true
      }
    })


		/* * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark
		 * Streaming 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
		 *
		 * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
		 * output
		 * Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
		 * 方法是foraeachRDD,因为Spark
		 * Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
		 * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
		 **/


    filteredClickInBatch.foreachRDD(rdd => {
      if (rdd.isEmpty()) {}  //hankl 2018-04-25 这里确实有必要判断一下，有可能是没有数据的
      rdd.foreachPartition(partition => {
        /**
         * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
         * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
         * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
         * 插入的用户信息可以只包含：timestamp、ip、userID、adID、province、city
         * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
         */
        val userAdClickedList = ListBuffer[UserAdClicked]()
        while (partition.hasNext) {
          val record = partition.next()
          val splited = record._1.split("_")
          val userClicked = new UserAdClicked()
          userClicked.timestamp = splited(0)
          userClicked.ip = splited(1)
          userClicked.userID = splited(2)
          userClicked.adID = splited(3)
          userClicked.province = splited(4)
          userClicked.city = splited(5)
          userAdClickedList += userClicked
        }

        val inserting = ListBuffer[UserAdClicked]()
        val updating = ListBuffer[UserAdClicked]()
        val  jdbcWrapper  =  JDBCWrapper.getInstance()

        // adclicked
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount

        for (clicked <- userAdClickedList) {
          def clickedquerycallBack(result: ResultSet): Unit = {
            while (result.next()) {
          //    println("result.getRow=====191==:   "+result.getRow)
              if ((result.getRow-1) != 0) {
                val count = result.getLong(1)
                clicked.clickedCount = count
                updating += clicked
             //    println("updateRecord=======195==============="  )

              } else {
                clicked.clickedCount = 0L
                inserting += clicked
             //    println("inserting============200=============="  )

              }
            }
          }

          jdbcWrapper.doQuery("SELECT count(1) FROM adclicked WHERE "
            + " timestamp = ? AND userID = ? AND adID = ?", Array(clicked.timestamp,
            clicked.userID, clicked.adID), clickedquerycallBack)
        }

        // adclicked java 397 line
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount

//2016.9.2 add class paramsList()
        val insertParametersList = ListBuffer[paramsList]()
        for (inserRecord <- inserting) {
         /* insertParametersList += Array(inserRecord.timestamp, inserRecord.ip,
            inserRecord.userID, inserRecord.adID, inserRecord.province,
            inserRecord.city, inserRecord.clickedCount)
           */

        //  println("insertParametersList" +inserRecord.timestamp)
          val  paramsListTmp =new paramsList()
          paramsListTmp.params1= inserRecord.timestamp
          paramsListTmp.params2 = inserRecord.ip
          paramsListTmp.params3 =  inserRecord.userID
          paramsListTmp.params4 = inserRecord.adID
          paramsListTmp.params5 = inserRecord.province
          paramsListTmp.params6 =  inserRecord.city
          paramsListTmp.params10_Long = inserRecord.clickedCount
          paramsListTmp.params_Type ="adclickedInsert"
          insertParametersList +=paramsListTmp
        }

        jdbcWrapper.doBatch("INSERT INTO adclicked VALUES(?,?,?,?,?,?,?)", insertParametersList)

        // adclicked java 407
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
        val updateParametersList = ListBuffer[paramsList]()
        for (updateRecord <- updating) {
         /* updateParametersList += Array(updateRecord.timestamp, updateRecord.ip,
            updateRecord.userID, updateRecord.adID, updateRecord.province,
            updateRecord.city, updateRecord.clickedCount)*/
        //   println("updateRecord" +updateRecord.timestamp)
             val  paramsListTmp =new paramsList()
          paramsListTmp.params1= updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.ip
          paramsListTmp.params3 =  updateRecord.userID
          paramsListTmp.params4 = updateRecord.adID
          paramsListTmp.params5 = updateRecord.province
          paramsListTmp.params6 =  updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type ="adclickedUpdate"
          updateParametersList +=paramsListTmp
        }

          jdbcWrapper.doBatch("UPDATE adclicked set clickedCount = ? WHERE "
          + " timestamp = ? AND ip = ? AND userID = ? AND adID = ? AND province = ? "
          + "AND city = ? ", updateParametersList)

      })
    })



    val blackListBasedOnHistory = filteredClickInBatch.filter(v1 => {
      val splited = v1._1.split("_")
      val date = splited(0)
      val userID = splited(2)
      val adID = splited(3)
      /**
       * 接下来根据date、userID、adID等条件去查询用户点击广告的数据表，获得总的点击次数
       * 这个时候基于点击次数判断是否属于黑名单点击 *
       */
      val clickedCountTotalToday = 81
      if (clickedCountTotalToday > 1) {
        true
      } else {
        false
      }
    })
    /**
     * 必须对黑名单的整个RDD进行去重操作！！！return v1._1.split("_")[2]
     */
    val blackListuserIDtBasedOnHistory = blackListBasedOnHistory.map(_._1.split("_")(2))

    val blackListUniqueuserIDtBasedOnHistory = blackListuserIDtBasedOnHistory.transform(_.distinct())
    // 下一步写入黑名单数据表中
    blackListUniqueuserIDtBasedOnHistory.foreachRDD(rdd => {
      /**
       * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
       * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
       * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
       * 插入的用户信息可以只包含：useID 此时直接插入黑名单数据表即可。
       */
      rdd.foreachPartition(t => {
        val blackList = ListBuffer[paramsList]()
        while (t.hasNext) {
          //blackList += Array(t.next())
           val  paramsListTmp =new paramsList()
          paramsListTmp.params1= t.next()

          paramsListTmp.params_Type = "blacklisttableInsert"
           blackList +=paramsListTmp

        }
        val  jdbcWrapper  =  JDBCWrapper.getInstance()
        jdbcWrapper.doBatch("INSERT INTO blacklisttable VALUES (?) ", blackList)
      })
    })
    /**
     * 广告点击累计动态更新,每个updateStateByKey都会在Batch Duration的时间间隔的基础上进行更高点击次数的更新，
     * 更新之后我们一般都会持久化到外部存储设备上，在这里我们存储到MySQL数据库中；
     */
    val filteredadClickedStreamingmappair = filteredadClickedStreaming.map(t => {
      val splited = t._2.split("\t")
      val timestamp = splited(0) // yyyy-MM-dd
      val ip = splited(1)
      val userID = splited(2)
      val adID = splited(3)
      val province = splited(4)
      val city = splited(5)

      val clickedRecord = timestamp + "_" + adID + "_" + province + "_" + city

      (clickedRecord, 1L)

    })
    val updateFunc = (values: Seq[Long], state: Option[Long]) => {

      Some[Long](values.sum + state.getOrElse(0L))

    }

    val updateStateByKeyDStream: DStream[(String, Long)] = filteredadClickedStreamingmappair.updateStateByKey(updateFunc)

    updateStateByKeyDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        /**
         * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
         * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
         * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
         * 插入的用户信息可以只包含：timestamp、adID、province、city
         *
          * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
         */

        val adClickedList = ListBuffer[AdClicked]()
        while (partition.hasNext) {
          val record = partition.next()
          val splited = record._1.split("_")
          val adClicked = new AdClicked()
          adClicked.timestamp = splited(0)
          adClicked.adID = splited(1)
          adClicked.province = splited(2)
          adClicked.city = splited(3)
          adClicked.clickedCount = record._2
          adClickedList += adClicked
        }

        val inserting = ListBuffer[AdClicked]()
        val updating = ListBuffer[AdClicked]()
        val  jdbcWrapper  =  JDBCWrapper.getInstance()
        // adclicked
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
        for (clicked <- adClickedList) {

          def adClickedquerycallBack(result: ResultSet): Unit = {
            while (result.next()) {
              if ((result.getRow-1) != 0) {
                val count = result.getLong(1)
                clicked.clickedCount = count
                updating += clicked
              } else {
                // clicked.clickedCount = 0L
                inserting += clicked
              }
            }
          }
          jdbcWrapper.doQuery(
            "SELECT count(1) FROM adclickedcount WHERE "
              + " timestamp = ? AND adID = ? AND province = ? AND city = ? ",
            Array(clicked.timestamp, clicked.adID, clicked.province,
              clicked.city), adClickedquerycallBack)
        }
        // adclicked
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
        val insertParametersList = ListBuffer[paramsList]()
        for (inserRecord <- inserting) {
          /*insertParametersList += Array(inserRecord.timestamp, inserRecord.adID, inserRecord.province,
            inserRecord.city, inserRecord.clickedCount)*/
           val  paramsListTmp =new paramsList()
          paramsListTmp.params1= inserRecord.timestamp
          paramsListTmp.params2 = inserRecord.adID
          paramsListTmp.params3 = inserRecord.province
          paramsListTmp.params4 =  inserRecord.city
          paramsListTmp.params10_Long = inserRecord.clickedCount
          paramsListTmp.params_Type ="adclickedcountInsert"
          insertParametersList +=paramsListTmp

        }
        jdbcWrapper.doBatch("INSERT INTO adclickedcount VALUES(?,?,?,?,?)", insertParametersList)
        // adclicked
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
        val updateParametersList = ListBuffer[paramsList]()
        for (updateRecord <- updating) {
         /* updateParametersList += Array(updateRecord.clickedCount,
            updateRecord.timestamp, updateRecord.adID, updateRecord.province,
            updateRecord.city)*/
               val  paramsListTmp =new paramsList()
          paramsListTmp.params1= updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.adID
          paramsListTmp.params3 = updateRecord.province
          paramsListTmp.params4 =  updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type ="adclickedUpdate"
          updateParametersList +=paramsListTmp

        }
        jdbcWrapper.doBatch(
          "UPDATE adclickedcount set clickedCount = ? WHERE "
            + " timestamp = ? AND adID = ? AND province = ? AND city = ? ",
          updateParametersList)

      })
    })
    /**
     * 对广告点击进行TopN的计算，计算出每天每个省份的Top5排名的广告； 因为我们直接对RDD进行操作，所以使用了transform算子；
     */
    val updateStateByKeyDStreamrdd = updateStateByKeyDStream.transform(rdd => {

      val rowRDD = rdd.map(t => {

        val splited = t._1.split("_")
        val timestamp = "2016-09-03" // yyyy-MM-dd
        val adID = splited(1)
        val province = splited(2)
        val clickedRecord = timestamp + "_" + adID + "_" + province
        (clickedRecord, t._2)

      }).reduceByKey(_ + _).map(v1 => {
        val splited = v1._1.split("_")
        val timestamp = "2016-09-03" // yyyy-MM-dd
        val adID = splited(1)
        val province = splited(2)
        Row(timestamp, adID, province, v1._2)

      })
      val structType = new StructType()
        .add("timstamp", StringType)
        .add("adID", StringType)
        .add("province", StringType)
        .add("clickedCount", LongType)

      val hiveContext = new HiveContext(rdd.sparkContext)
      val df = hiveContext createDataFrame(rowRDD, structType)
      df registerTempTable "topNTableSource"
      val sqlText = "SELECT timstamp,adID,province,clickedCount FROM " +
        " ( SELECT timstamp,adID,province,clickedCount, row_number() " +
        " OVER ( PARTITION BY province ORDER BY clickedCount DESC ) rank " +
        " FROM topNTableSource ) subquery " + " WHERE rank <= 5 "
      val result = hiveContext.sql(sqlText)
      result.rdd

    })

    updateStateByKeyDStreamrdd.foreachRDD(rdd => {
      rdd.foreachPartition(t => {
        val adProvinceTopN = ListBuffer[AdProvinceTopN]()
        while (t.hasNext) {
          val row = t.next()
          val item = new AdProvinceTopN();
          item.timestamp = row.getString(0)
          item.adID = row.getString(1)
          item.province = row.getString(2)
          item.clickedCount = row.getLong(3)
          adProvinceTopN += item
        }
        val  jdbcWrapper  =  JDBCWrapper.getInstance()
        val set = new mutable.HashSet[String]()
        for (itemTopn <- adProvinceTopN) {
          set += itemTopn.timestamp + "_" + itemTopn.province
        }
        // adclicked
        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
        val deleteParametersList = ListBuffer[paramsList]()
        for (deleteRecord <- set) {
          val splited = deleteRecord.split("_")
          //deleteParametersList += Array(splited(0), splited(1))
            val  paramsListTmp =new paramsList()
          paramsListTmp.params1= splited(0)
          paramsListTmp.params2 = splited(1)
          paramsListTmp.params_Type ="adprovincetopnDelete"
          deleteParametersList +=paramsListTmp

        }
        jdbcWrapper.doBatch("DELETE FROM adprovincetopn WHERE timestamp = ? AND province = ?",
          deleteParametersList);
        val insertParametersList = ListBuffer[paramsList]()
        for (updateRecord <- adProvinceTopN) {
          /*insertParametersList += Array(updateRecord.timestamp, updateRecord.adID,
            updateRecord.province, updateRecord.clickedCount)*/
            val  paramsListTmp =new paramsList()
          paramsListTmp.params1= updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.adID
          paramsListTmp.params3 = updateRecord.province
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type ="adprovincetopnInsert"
          insertParametersList +=paramsListTmp

        }
        jdbcWrapper.doBatch("INSERT INTO adprovincetopn VALUES (?,?,?,?) ", insertParametersList)

      })
    })
    /**
     * 计算过去半个小时内广告点击的趋势 用户广告点击信息可以只包含：timestamp、ip、userID、adID、province、city
     */
    val filteredadClickedStreamingpair = filteredadClickedStreaming.map(t => {
      val splited = t._2.split("\t")
      val adID = splited(3)
      val time = splited(0) // Todo：后续需要重构代码实现时间戳和分钟的转换提取，此处需要提取出该广告的点击分钟单位
      (time + "_" + adID, 1L)

    })
    filteredadClickedStreamingpair.reduceByKeyAndWindow(_ + _, _ - _, Seconds(1800), Seconds(60))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val adTrend = ListBuffer[AdTrendStat]()
          while (partition.hasNext) {
            val record = partition.next()
            val splited = record._1.split("_")
            val time = splited(0)
            val adID = splited(1)
            val clickedCount = record._2
            /**
             * 在插入数据到数据库的时候具体需要哪些字段？time、adID、clickedCount；
             * 而我们通过J2EE技术进行趋势绘图的时候肯定是需要年、月、日、时、分这个维度的，所有
             * 我们在这里需要年月日、小时、分钟这些时间维度；
             */
            val adTrendStat = new AdTrendStat()
            adTrendStat.adID = adID
            adTrendStat.clickedCount = clickedCount
            adTrendStat._date = time // Todo:获取年月日
            adTrendStat._hour = time // Todo:获取小时
            adTrendStat._minute = time // Todo:获取分钟
            adTrend += adTrendStat
          }
          val inserting = ListBuffer[AdTrendStat]()
          val updating = ListBuffer[AdTrendStat]()
          val  jdbcWrapper  =  JDBCWrapper.getInstance()
          // adclickedtrend
          // 表的字段：date、hour、minute、adID、clickedCount
          for (clicked <- adTrend) {
            val adTrendCountHistory = new AdTrendCountHistory()

            def adTrendquerycallBack(result: ResultSet): Unit = {
              while (result.next()) {
                if ((result.getRow-1) != 0) {
                  val count = result.getLong(1)
                  adTrendCountHistory.clickedCountHistory = count
                  println("adclicked=====trend===updating=============: "+ count)
                  updating += clicked
                } else {
                  inserting += clicked
                   println("adclicked=====trend===inserting=============: " + clicked.clickedCount)
                }
              }
            }

             jdbcWrapper.doQuery("SELECT count(1) FROM adclickedtrend WHERE "
											+ " date = ? AND hour = ? AND minute = ? AND adID = ?", Array(clicked._date,
            clicked._hour, clicked._minute,clicked.adID), adTrendquerycallBack)




          }
          val insertParametersList = ListBuffer[paramsList]()

          for (inserRecord <- inserting) {
          //  insertParametersList += Array(inserRecord._date, inserRecord._hour, inserRecord._minute, inserRecord.adID, inserRecord.clickedCount)

           val  paramsListTmp =new paramsList()
          paramsListTmp.params1= inserRecord._date
          paramsListTmp.params2 = inserRecord._hour
          paramsListTmp.params3 =  inserRecord._minute
          paramsListTmp.params4 = inserRecord.adID
          paramsListTmp.params10_Long = inserRecord.clickedCount
          paramsListTmp.params_Type ="adclickedtrendInsert"
          insertParametersList +=paramsListTmp

          }
          jdbcWrapper.doBatch("INSERT INTO adclickedtrend VALUES(?,?,?,?,?)", insertParametersList)

          val updateParametersList = ListBuffer[paramsList]()
          for (updateRecord <- updating) {
           // updateRecord.adID
           /* updateParametersList += Array(updateRecord.clickedCount,
              updateRecord._date, updateRecord._hour, updateRecord._minute,
              updateRecord.adID)*/

             val  paramsListTmp =new paramsList()
          paramsListTmp.params1= updateRecord._date
          paramsListTmp.params2 = updateRecord._hour
          paramsListTmp.params3 =  updateRecord._minute
          paramsListTmp.params4 = updateRecord.adID
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type ="adclickedtrendUpdate"
          updateParametersList +=paramsListTmp
          }
          jdbcWrapper.doBatch("UPDATE adclickedtrend set clickedCount = ? WHERE "
            + " date = ? AND hour = ? AND minute = ? AND adID = ?", updateParametersList)

        }) //   rdd.foreachPartition(partition => {

      }) //  .foreachRDD(rdd =>{


    ssc.start()
    ssc.awaitTermination()

  }


  object JDBCWrapper {

    private var jdbcInstance: JDBCWrapper = _

    def getInstance(): JDBCWrapper = {
      synchronized {

      if (jdbcInstance == null) {

        jdbcInstance = new JDBCWrapper()
         }
      }
      jdbcInstance
    }

  }

  class JDBCWrapper {

     val dbConnectionPool = new LinkedBlockingQueue[Connection]()
    try {
      Class.forName("com.mysql.jdbc.Driver") //hankl注册驱动
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }

    for (i <- 1 to 10) {
      try {
        val conn = DriverManager.getConnection("jdbc:mysql://Master:3306/sparkstreaming", "root",
          "root");
        dbConnectionPool.put(conn);
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }



    //todo
    def   getConnection(): Connection = synchronized {
      while (0 == dbConnectionPool.size()) {
        try {
          Thread.sleep(20);
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
      dbConnectionPool.poll();
    }


    //def doBatch(sqlText: String, paramsList: ListBuffer[Array[_]]): Array[Int] = {
//2016.9.2
    def doBatch(sqlText: String, paramsList: ListBuffer[paramsList]): Array[Int] = {

      val conn: Connection = getConnection();
      var preparedStatement: PreparedStatement = null;
      val result: Array[Int] = null;
      try {
        conn.setAutoCommit(false);
        preparedStatement = conn.prepareStatement(sqlText)
           //  println("====doBatch  sqlText:   "+sqlText)
        for (parameters <- paramsList) {
          println("====doBatch parameters.params_Type:   "+parameters.params_Type)

       //2016.9.2 add class paramsList()
         parameters.params_Type match{
           //比一比谁更 傻
          case "adclickedInsert" => { println("adclickedInsert")

          //   println("====doBatch adclickedInsert  parameters.params1:   "+parameters.params1)
           //   println("====doBatch adclickedInsert  parameters.params10_Long:   "+parameters.params10_Long)
                preparedStatement.setObject(1, parameters.params1)
                preparedStatement.setObject(2, parameters.params2)
                preparedStatement.setObject(3, parameters.params3)
                preparedStatement.setObject(4, parameters.params4)
                preparedStatement.setObject(5, parameters.params5)
                preparedStatement.setObject(6, parameters.params6)
                preparedStatement.setObject(7, parameters.params10_Long)
                }

          case  "blacklisttableInsert" => { println("blacklisttableInsert")
                preparedStatement.setObject(1, parameters.params1)
                }
           case  "adclickedcountInsert" => { println("adclickedcountInsert")
                preparedStatement.setObject(1, parameters.params1)
                preparedStatement.setObject(2, parameters.params2)
                preparedStatement.setObject(3, parameters.params3)
                preparedStatement.setObject(4, parameters.params4)
                preparedStatement.setObject(5, parameters.params10_Long)
                }
            case  "adprovincetopnInsert" => { println("adprovincetopnInsert")
                preparedStatement.setObject(1, parameters.params1)
                preparedStatement.setObject(2, parameters.params2)
                preparedStatement.setObject(3, parameters.params3)
                preparedStatement.setObject(4, parameters.params10_Long)
                }
             case  "adclickedtrendInsert" => { println("adclickedtrendInsert")
               println("====doBatch adclickedtrendInsert  parameters.params1:   "+parameters.params1)
               println("====doBatch adclickedtrendInsert  parameters.params2:   "+parameters.params2)
                println("====doBatch adclickedtrendInsert  parameters.params10_Long:   "+parameters.params10_Long)
                preparedStatement.setObject(1, parameters.params1)
                preparedStatement.setObject(2, parameters.params2)
                preparedStatement.setObject(3, parameters.params3)
                preparedStatement.setObject(4, parameters.params4)
                preparedStatement.setObject(5, parameters.params10_Long)
                }
            //再来比一比谁更傻
             //还要考虑更新update的情况，顺序不同了
             case "adclickedUpdate" => { println("adclickedUpdate")
                //还要考虑更新update的情况，顺序不同了
               // println("====doBatch adclickedUpdate parameters.params1:   "+parameters.params1)
              //  println("====doBatch  adclickedUpdate parameters.params10_Long:   "+parameters.params10_Long)
                preparedStatement.setObject(1, parameters.params10_Long)
                preparedStatement.setObject(2, parameters.params1)
                preparedStatement.setObject(3, parameters.params2)
                preparedStatement.setObject(4, parameters.params3)
                preparedStatement.setObject(5, parameters.params4)
                preparedStatement.setObject(6, parameters.params5)
                preparedStatement.setObject(7, parameters.params6)

                }

          case  "blacklisttableUpdate" => { println("blacklisttableUpdate")
                preparedStatement.setObject(1, parameters.params1)
                }
           case  "adclickedcountUpdate" => { println("adclickedcountUpdate")
                preparedStatement.setObject(1, parameters.params10_Long)
                preparedStatement.setObject(2, parameters.params1)
                preparedStatement.setObject(3, parameters.params2)
                preparedStatement.setObject(4, parameters.params3)
                preparedStatement.setObject(5, parameters.params4)

                }
            case  "adprovincetopnUpdate" => { println("adprovincetopnUpdate")
                preparedStatement.setObject(1, parameters.params10_Long)
                preparedStatement.setObject(2, parameters.params1)
                preparedStatement.setObject(3, parameters.params2)
                preparedStatement.setObject(4, parameters.params3)

                }

             case  "adprovincetopnDelete" => { println("adprovincetopnDelete")

                preparedStatement.setObject(1, parameters.params1)
                preparedStatement.setObject(2, parameters.params2)


                }

             case  "adclickedtrendUpdate" => { println("adclickedtrendUpdate")
                preparedStatement.setObject(1, parameters.params10_Long)
                preparedStatement.setObject(2, parameters.params1)
                preparedStatement.setObject(3, parameters.params2)
                preparedStatement.setObject(4, parameters.params3)
                preparedStatement.setObject(5, parameters.params4)
                }

           } //case match

          preparedStatement.addBatch();
        }

        val result = preparedStatement.executeBatch();

        conn.commit();
      } catch {
        // TODO Auto-generated catch block
        case e: Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) {
          try {
            preparedStatement.close();
          } catch {
            // TODO Auto-generated catch block
            case e: SQLException => e.printStackTrace()
          }
        }

        if (conn != null) {
          try {
            dbConnectionPool.put(conn);
          } catch {
            // TODO Auto-generated catch block
            case e: InterruptedException => e.printStackTrace()
          }
        }
      }

      result;
    }
    def doQuery(sqlText: String, paramsList: Array[_], callBack: ResultSet => Unit) {

      val conn: Connection = getConnection();
      var preparedStatement: PreparedStatement = null;
      var result: ResultSet = null;
      try {
        preparedStatement = conn.prepareStatement(sqlText)
   //println("==doQuery===sqlText:  "+sqlText)
        if (paramsList != null) {

          for (i <- 0 to paramsList.length - 1) {
        //    println("==doQuery===paramsList.length +paramsList(i) :  "+paramsList.length + "   "+paramsList(i))
            preparedStatement.setObject(i + 1, paramsList(i))

          }
        }
        result = preparedStatement.executeQuery()
             println( " doQuery=====result.getRow=====:   "  + result.getRow)
        callBack(result)

      } catch {
        // TODO Auto-generated catch block
        case e: Exception => e.printStackTrace()

      } finally {
        if (preparedStatement != null) {
          try {
            preparedStatement.close();
          } catch {
            // TODO Auto-generated catch block
            case e: SQLException => e.printStackTrace()

          }
        }

        if (conn != null) {
          try {
            dbConnectionPool.put(conn);
          } catch {
            // TODO Auto-generated catch block
            case e: InterruptedException => e.printStackTrace()

          }
        }
      }

    }
  }

  def resultCallBack(result: ResultSet, blackListNames: List[String]): Unit = {

  }
  //2016.9.2 add class paramsList()
  class paramsList extends Serializable {
    var params1: String = _
    var params2: String = _
    var params3: String = _
    var params4: String = _
    var params5: String = _
    var params6: String = _
    var params7: String = _
    var params10_Long: Long = _
     var params_Type : String = _
    var length: Int = _

  }

  class UserAdClicked extends Serializable {
    var timestamp: String = _
    var ip: String = _
    var userID: String = _
    var adID: String = _
    var province: String = _
    var city: String = _
    var clickedCount: Long = _

    override def toString: String = "UserAdClicked [timestamp=" +
      timestamp + ", ip=" + ip + ", userID=" + userID + ", adID=" +
      adID + ", province=" + province + ", city=" + city + ", clickedCount=" +
      clickedCount + "]";

  }

  class AdClicked extends Serializable {
    var timestamp: String = _
    var adID: String = _
    var province: String = _
    var city: String = _
    var clickedCount: Long = _
    override def toString: String = "AdClicked [timestamp=" +
      timestamp + ", adID=" + adID + ", province=" +
      province + ", city=" + city + ", clickedCount=" +
      clickedCount + "]"
  }

  class AdProvinceTopN extends Serializable {
    var timestamp: String = _
    var adID: String = _
    var province: String = _
    var clickedCount: Long = _
  }

  class AdTrendStat extends Serializable {
    var _date: String = _
    var _hour: String = _
    var _minute: String = _
    var adID: String = _
    var clickedCount: Long = _
    override def toString: String = "AdTrendStat [_date=" +
      _date + ", _hour=" + _hour + ", _minute=" + _minute + ", adID=" +
      adID + ", clickedCount=" + clickedCount + "]"
  }

  class AdTrendCountHistory extends Serializable {
    var clickedCountHistory: Long = _
  }




}*/
