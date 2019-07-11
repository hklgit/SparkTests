package redis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
  * Created by Kerven on 2019/7/4.
  * Talk is cheap , show me the code 
  *
  *
  * ref ：https://blog.csdn.net/weixin_42003671/article/details/88353506
  * ref : https://github.com/RedisLabs/spark-redis
  */
object SparkReadFromRedis {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", "file:///")
      .config("spark.redis.host", "192.168.1.47")
      .config("spark.redis.port", "6543")
      //      .config("spark.redis.auth", "6543")
      .config("spark.redis.timeout", 500)
      //      .config("spark.redis.auth", "aaron227") //指定redis密码
      .config("spark.redis.db", "1") //指定redis库
      .enableHiveSupport()
      .getOrCreate()


    //write data to redis
    val personSeq = Seq(Person("Aaron", 30), Person("Peter", 45))
    val dfW = spark.createDataFrame(personSeq)
    dfW.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      //      .option("key.column", "name")
      .option("ttl", 30) // set the ttl
      //      .mode(SaveMode.Overwrite)
      .save()


    val dfR = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .option("key.column", "name")
      .option("keys.pattern", "person:*")
      .load()
    println("====================")
    dfR.show(2)
    println("====================")


    //if u want to assign the schema
    val loadedDf3 = spark.read
      .format("org.apache.spark.sql.redis")
      .schema(StructType(Array(StructField("id", IntegerType),
        StructField("name", StringType), StructField("age", IntegerType))))
      .option("keys.pattern", "person:*")
      .option("key.column", "name")
      .load()
    loadedDf3.show(false)


    spark.stop()


  }
}
