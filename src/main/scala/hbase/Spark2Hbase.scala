package com.coocaa.hbase

import com.coocaa.common.DateUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


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
  * spark write data to hbase
  *
  */
object Spark2Hbase {

  def main(args: Array[String]): Unit = {
    /**
      * 导入配置
      */
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}".filter(!_.equals('$')))
    conf.set("spark.network.timeout", "10000000")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.speculation.interval", "10000ms")
    conf.set("spark.sql.tungsten.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", "800")
    conf.set("spark.Kryoserializer.buffer.max", "1024m")
    conf.set("spark.sql.warehouse.dir", "hdfs://XXXX")
    conf.set("spark.sql.broadcastTimeout", "5400")
    conf.set("spark.default.parallelism", "800")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //hbase 的配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource(this.getClass.getResourceAsStream("/hbase-site.xml"))//因为牵涉到集群的配置信息，没有上传，放到resource下即可
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"MyTestTablehkl") //表我已经提前建好了，也可以在代码里创建，这里是为了方便

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")

    /**
      * 时间模块在这里
      */
    val date_Str = conf.get("spark.input.date", DateUtil.getDateBeforeDay)
    val n_day_before = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(date_Str, "yyyy-MM-dd"), -30), "yyyy-MM-dd")
    val table_dt = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(date_Str, "yyyy-MM-dd"), 1), "yyyy-MM-dd")


    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    /*/**
      * saveAsHadoopDataset  jobConf
      * 1. 指定key的输出类型
      */
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定表名称
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "MyTestTable")*/

    // data source
    val  dataDF= spark.sql(
      s"""
         xxxx
      """.stripMargin)

    //    println("total number of data" + dataDF.count())
    println("/////////////////////////////")
    dataDF.show(10)
    println("/////////////////////////////")


    // 业务逻辑
    val totalCount = dataDF.rdd.map(row => {

      // 行数据进行标签化处理
      val usrMac: String = row.getAs[String]("mac")
      val tagVideo: Map[String, Int] = Tags4Video.makeTags(row)

      (usrMac, tagVideo.toList)
    }).reduceByKey((a, b) => {
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).map{
    case (usrMac, usrTag) => {
      // 放入rowkey
      val put = new Put(Bytes.toBytes(usrMac))
      // 把一堆标签做为一个整体的值存入到列里面
      val tags: String = usrTag.map(t => t._1+":"+t._2).mkString(",")
      // 列族, 列, 值
      // 每天一列
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(s"day$date_Str"), Bytes.toBytes(tags))

      // rowkey 指定一个输出类型
      (new ImmutableBytesWritable(), put)  // ImmutableBytesWritable 修饰rowkey对象
    }
  }.saveAsNewAPIHadoopDataset(job.getConfiguration) // 这里是要说明的，我们使用的是新版本的api效果会好很多，推荐用这个.实测差距很大和老版本的


    println("program completely done! progress successfully :-)")

    spark.stop()




  }

  /**
    * 初始化和hbase相关的配置
    *
    * @param hTableName 表名
    * @param cf         对应的列族
    */
  def createHTable(hbaseConf: Configuration, hTableName: String, cf: String) {
    var conn: Connection = null
    var admin: Admin = null
    try {
      conn = ConnectionFactory.createConnection(hbaseConf)
      admin = conn.getAdmin
      if (!admin.tableExists(TableName.valueOf(hTableName))) {
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(hTableName))
        tableDescriptor.addFamily(new HColumnDescriptor(cf))
        admin.createTable(tableDescriptor)
      }else{
        println("table already exits")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      admin.close()
      conn.close()
    }

  }
}




//}
