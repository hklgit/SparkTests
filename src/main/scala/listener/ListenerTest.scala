package listener

import org.apache.spark.sql.SparkSession

/**
  * Created by Kerven on 2019/4/11.
  */
object ListenerTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getSimpleName}").config("spark.extraListeners","com.coocaa.listener.MySparkAppListener")
      .getOrCreate()
    println("==============================")
    spark.stop()
  }
}
