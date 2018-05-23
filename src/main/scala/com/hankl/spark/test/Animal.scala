package com.hankl.spark.test

/**
  * Created by hankl on 2018/5/11.
  */
class Animal extends  Earth{
  override def sound(){
    println("animal sound ")

  }

  //定义了一个方法 T必须是Animal的子类 T <:Animal
  /**
    * 这个方法我们可以直接在object里去设定，如果只在class丽的话那么就new一下
    * 就可以调用了在object中
    * @param things
    * @tparam T
    * @return
    */
  def biophony[T <:Animal](things:Seq[T])=things.map(_.sound)

}

object Animal extends  Earth{





  def main(args: Array[String]): Unit = {
      val animal= new Animal
      animal.biophony(Seq(new Dog,new Dog))
  }


}
