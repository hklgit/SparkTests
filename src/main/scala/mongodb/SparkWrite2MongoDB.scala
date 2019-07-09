package mongodb

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
  * Created by Kerven on 2019/7/9.
  * Talk is cheap , show me the code 
  * ref link:  https://blog.csdn.net/qq_33689414/article/details/83421766
  */
object SparkWrite2MongoDB {

  def main(args: Array[String]): Unit = {


    //    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.user")

    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName)
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.98/test.coocaa")
      .enableHiveSupport().getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val document1 = new Document()
    document1.append("name", "mayun").append("age", 18).append("sex", "female")
    val document2 = new Document()
    document2.append("name", "mahuateng").append("age", 24).append("sex", "female")
    val document3 = new Document()
    document3.append("name", "liyanhong").append("age", 23).append("sex", "female")

    val seq = Seq(document1, document2, document3)
    val df = spark.sparkContext.parallelize(seq)

    // 将数据写入mongo
    MongoSpark.save(df)


    spark.stop()
  }


}
