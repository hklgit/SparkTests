package com.hankl.spark.test

/**
  * Created by hankl on 2018/4/27.
  */
class SecondSort(var a:Int,var b:Int) extends Ordered[SecondSort] with Serializable{
  override def compare(that: SecondSort): Int = {
    if(this.a-that.a !=0){
      this.a-that.a
    }else{
      this.b-that.b
    }

  }
}


class SecondSorted(var a :Int,var b:Int) extends  Ordered[SecondSorted] {
  override def compare(that: SecondSorted): Int = {

      if(this.a-that.a != 0)
        this.a-that.a
      else this.b-that.b


  }
}