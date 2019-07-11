package hbase

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Try}


/**
  * Created by Kerven-HAN on 2019/7/3.
  *
  * ref : https://blog.csdn.net/weixin_42003671/article/details/91877377
  * https://www.cnblogs.com/itboys/p/9156789.html
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
  * Talk is cheap , show me the code
  *
  * some utils for spark handle hbase ---  kerven-HAN
  * 这个是给公司写的一个工具类。自己也做个备份
  */
object HbaseSparkUtils {

  private var conn: Connection = _

  private var conf: Configuration = _

  private var admin: HBaseAdmin = _


  /**
    * init the configuration
    */
  def init() {

    try {
      conf = HBaseConfiguration.create()
      // load the config file
      conf.addResource(this.getClass.getResourceAsStream("hbase-site.xml"))
      // get the  connection
      conn = ConnectionFactory.createConnection(conf)
      // get the admin
      admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

    } catch {
      case e: Exception => new Throwable("========== init failed ,please check your code =========" + e)
    }


  }


  def getDataRddFromHbaseWithSparkAPI(tbName: String, spark: SparkSession): RDD[(ImmutableBytesWritable, Result)] = {

    if (null == conf) {
      println("init the config")
      init()
    }
    conf.set(TableInputFormat.INPUT_TABLE, tbName)
    val hbaseRdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRdd

  }

  /**
    *
    * @param tbName name of the table
    * @param rdd    the type of the rdd must be RDD[(ImmutableBytesWritable, Put)]
    *               note: To ensure flexibility, only one RDD[(ImmutableBytesWritable, Put)] is passed in here
    *               kerven-HAN
    */
  def insertData2HbaseWithSparkAPI(tbName: String, rdd: RDD[(ImmutableBytesWritable, Put)]) {

    try {
      if (null == conf) {
        println("init the config")
        init()
      }
      conf.set(TableOutputFormat.OUTPUT_TABLE, tbName)
      // get the job
      val job = Job.getInstance(conf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    } catch {
      case e: Exception => new Throwable("==============insert data failed ===========" + e)
    }

  }

  /**
    * use bulkload data to htable
    *
    * @param tableName
    * @param rdd
    * @return
    */
  def insertData2HbaseWithSparkAPIBulkLoad(tableName: String, dataPath: String, rdd: RDD[(ImmutableBytesWritable, Put)]) = {

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val _conn = ConnectionFactory.createConnection(conf)
    val load = new LoadIncrementalHFiles(conf)
    val hTableName = TableName.valueOf(tableName)
    val table = _conn.getTable(hTableName)
    val regionLocator = _conn.getRegionLocator(hTableName)
    val hdfsFile = dataPath
    val path = new Path(hdfsFile)
    val fileSystem = FileSystem.get(URI.create(hdfsFile), conf)
    // if the dir already exists , delete it and recreate the dir
    if (fileSystem.exists(path)) {
      fileSystem.delete(new Path(hdfsFile), true)
      Try(fileSystem.create(path)) match {
        case _ => println("==================delete success,begin create hdfs dir successed")
        case Failure(e) => println("=============== create hdfs dir  failed =============== " + e)
      }
    } else {
      // if the dir don't exists create it
      Try(fileSystem.create(path)) match {
        case _ => println("==================do not found dir ,begin  create hdfs dir successed")
        case Failure(e) => println("=============== create hdfs dir  failed =============== " + e)
      }
    }
    rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[Result],
      classOf[TableOutputFormat[ImmutableBytesWritable]], conf)

    //    load.doBulkLoad(path,admin,table,regionLocator)
    load.doBulkLoad(new Path(dataPath), _conn.getAdmin, table, regionLocator)

  }



  /**
    * create htable
    *
    * @param hTableName tbl name
    * @param cf         column family
    */
  def createHTable(hTableName: String, cf: String) {
    var conn: Connection = null
    var admin: Admin = null
    if (null == conn) init()
    try {
      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin
      if (!admin.tableExists(TableName.valueOf(hTableName))) {
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(hTableName))
        tableDescriptor.addFamily(new HColumnDescriptor(cf))
        admin.createTable(tableDescriptor)
        println("create table successful")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      admin.close()
      conn.close()
    }

  }


  def close(table: Table, con: Connection): Unit = {
    if (table != null)
      table.close()
    if (con != null)
      con.close()
  }

  /**
    * insert data into hbase
    *
    * @param tableName
    * @param rowKey
    * @param columnFamily
    * @param column
    * @param value
    */
  def singleInsertData(tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {
    val con = ConnectionFactory.createConnection(conf)
    val table = con.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
    close(table, con)
  }

  /**
    * batch insert data into hbase
    *
    * @param tableName
    * @param rowKey
    * @param columnFamily
    * @param column
    * @param value
    */
  def batchInsertData(tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {
    val con = ConnectionFactory.createConnection(conf)
    val table: BufferedMutator = con.getBufferedMutator(TableName.valueOf(tableName))
    val p = new Put(Bytes.toBytes(rowKey))
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    val mutations = new java.util.ArrayList[Mutation]()
    mutations.add(p)
    table.mutate(mutations)
    table.flush()
    if (con != null)
      con.close()
    if (table != null)
      table.close()
  }

  /**
    * delete data from hbase
    *
    * @param tableName
    */
  def deleteData(tableName: String): Unit = {
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * scan all data from the htable
    *
    * @param tableName
    * @return Cell
    */
  def getDataByScan(tableName: String): ArrayBuffer[Array[Cell]] = {
    var arrayBuffer = ArrayBuffer[Array[Cell]]()
    val scanner = new Scan()
    val table = conn.getTable(TableName.valueOf(tableName))
    val results = table.getScanner(scanner)
    var res: Result = results.next()
    while (res != null) {
      arrayBuffer += res.rawCells()
      res = results.next()
    }
    arrayBuffer
  }

  /**
    * scan the data by specify  the start & end row
    *
    * @param tableName
    * @param startRowKey
    * @param stopRowKey
    */
  def getDataByScan(tableName: String, startRowKey: String, stopRowKey: String) = {

    val table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan().setStartRow(Bytes.toBytes(startRowKey)).setStopRow(Bytes.toBytes(stopRowKey))
    val rs = table.getScanner(scan)
    try {
      val resultScan = rs.iterator()
      while (resultScan.hasNext) {
        val result = resultScan.next().rawCells()
        for (i <- 0.until(result.length)) {
          val family = Bytes.toString(CellUtil.cloneFamily(result(i)))
          val rowKey = Bytes.toString(CellUtil.cloneRow(result(i)))
          val column = Bytes.toString(CellUtil.cloneQualifier(result(i)))
          val value = Bytes.toString(CellUtil.cloneValue(result(i)))
          println(s"$family:$rowKey,$column:$value")
        }
      }
    } catch {
      case e: Exception => new Throwable("=========== batch scan failed ================== :" + e)
    } finally {
      rs.close()
      table.close()
    }
  }


  /**
    * get per row by rowkey
    *
    * @param tableName
    * @param rowKey
    * @return Array[Cell]
    */
  def getRow(tableName: String, rowKey: String): Array[Cell] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    val res = table.get(get)
    res.rawCells()
  }

  /**
    * delete specify row by rowKey
    *
    * @param tableName
    * @param rowKey
    */
  def delRow(tableName: String, rowKey: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    table.delete(new Delete(Bytes.toBytes(rowKey)))
  }


}
