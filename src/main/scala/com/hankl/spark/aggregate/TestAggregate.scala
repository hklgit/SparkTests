package com.hankl.spark.aggregate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hankl on 2018/5/15.
  */
object TestAggregate {


  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")

    val sc=new SparkContext(conf)

    var data:RDD[(Int, Int)]=sc.parallelize(List((1,2),(1,3),(1,4),(1,5),(1,6)))
//    println(data.getNumPartitions)

    def seq(a:Int,b:Int): Int ={
      println("seq : "+a+"\t"+b)
      math.max(a,b)
    }

    def comb(a:Int,b:Int):Int={
        println("comb: "+a+"\t"+b)
      a+b
    }

//    data.aggregateByKey(1)(seq,comb).collect()









  }
}
