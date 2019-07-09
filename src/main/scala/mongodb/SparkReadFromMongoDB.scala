package mongodb

import com.mongodb.spark.MongoSpark
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
  * Created by Kerven on 2019/7/9.
  * Talk is cheap , show me the code 
  *
  */
object SparkReadFromMongoDB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName)
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.98/test.coocaa")
      .config("spark.sql.warehouse.dir", "file:///")
      .enableHiveSupport().getOrCreate()

//        val df = MongoSpark.load(spark)

    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("sex", StringType)
      )
    )


    // 通过schema约束，直接获取需要的字段
    val df = spark.read.format("com.mongodb.spark.sql").schema(schema).load()
    df.show()


    /*

        df.createOrReplaceTempView("user")

        val resDf = spark.sql("select name,age,sex from user")
        resDf.show()
    */

    spark.stop()


  }

}
