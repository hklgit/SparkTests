package hbase.demo

import com.coocaa.hbase.HbaseSparkUtils
import hbase.HbaseSparkUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
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
  * Created by Kerven on 2019/7/11.
  * Talk is cheap , show me the code
  *
  *  demo for insertData2HbaseWithSparkAPIBulkLoad
  *
  */
object SparkHbaseTest01 {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HbaseBulkLoad")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    HbaseSparkUtils.init()

    val datas = List(
      ("abc", ("ext", "type", "login")),
      ("ccc", ("ext", "type", "logout"))
    )
    val dataRdd = spark.sparkContext.parallelize(datas)

    val output = dataRdd.map{
      x=>{
        val rowkey = Bytes.toBytes(x._1)
        val put = new Put(rowkey)
        put.addColumn(Bytes.toBytes("ext"), Bytes.toBytes(x._2._2), Bytes.toBytes(x._2._3))
        (new ImmutableBytesWritable(), put)
      }
    }

    val dataPath = "/tmp/han"
    val tableName = "test0009"

    HbaseSparkUtils.insertData2HbaseWithSparkAPIBulkLoad(tableName, dataPath, output)

    spark.stop()


  }



}
