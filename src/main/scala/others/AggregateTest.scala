package others

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
  * 求均值，一个是求和，一个是统计元素的个数
  * ref：https://www.jianshu.com/p/640a7fc26303
  */
object AggregateTest {

  /**
    * 聚合每个分区里的元素
    * @param tuple1
    * @param num  每次要参与运算的数
    * @return
    */
  def sqlOp(tuple1: (Int, Int), num: Int): (Int, Int) = {
    (tuple1._1+num,tuple1._2+1)
  }

  /**
    * 聚合每个分区的结果集
    * @param tuple1
    * @param tuple2
    * @return
    */
  def combOp(tuple1: (Int,Int),tuple2: (Int,Int)): (Int,Int) = {
    (tuple1._1+tuple2._1,tuple1._2+tuple2._2);
  }

  def main(args: Array[String]): Unit = {

    val rdd = List(1,2,3,4,5,6,7,8,9)

    val resutlt = rdd.par.aggregate((0,0))(sqlOp,combOp);

    println(resutlt._1)
    println(resutlt._2)
    val avg = resutlt._1 / resutlt._2





  }

}
