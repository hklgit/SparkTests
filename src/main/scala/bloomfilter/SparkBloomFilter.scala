package bloomfilter

import org.apache.spark.sql.SparkSession

/**
  * Created by Kerven on 2019/7/3.
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
  * 背景:
  * 推荐部分生产上要过滤掉已经推荐过的电影，之前一直使用的是sql，为了达到更好的效果，我特意做了一个bloomfilter的调研
  * 更好的提升效率
  * 这里吧测试的过程给记录下来，供探讨
  *
  */
object SparkBloomFilter {

  val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getCanonicalName)
    .config("spark.sql.warehouse.dir", "file:///")
    .enableHiveSupport().getOrCreate()

  import spark.implicits._

  //模拟10亿 条数据
  val rddStr = spark.sparkContext.makeRDD(1 to 1000000000)
  println("==============" + rddStr.count())

  // 模拟被判断的数据，因为用的是string，所以这里做了处理
  val rddStr1Str = spark.sparkContext.makeRDD(100 to 1000000).map(_.toString)


  /**
    * 第一个参数是使用的数据列，
    * 第二个参数是数据量期望会有多少，
    * 第三个参数是损失精度。损失精度越低生成的布隆数组长度就会越长，占用的空间就会越多，计算过程就会越漫长
    */
  val bf = rddStr.map(i=>i.toString).toDF("mac").stat.bloomFilter("mac", 10000, 0.01)

  val boolRdd =  rddStr1Str.map(str=>{
    val bool =  bf.mightContain(str)
    (str,bool) //为了便于观察过滤的效果，我把对应的数据也做了保留
  }).filter(_._2.equals(true)).collect().foreach(println(_))

  /**
    * note:
    * 我在我本地测试的，内存有限，感觉效果不是太明显。
    * 在集群上申请的资源：
    * --executor-memory 5g --num-executors 60  --driver-memory=6g
    * 结果秒出，效果很不错。资源是否还可以调小，没有做测试。感兴趣的可以自己测试下。
    */



}
