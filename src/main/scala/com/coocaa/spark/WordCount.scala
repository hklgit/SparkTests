package com.coocaa.spark

import java.io.FileReader
import java.util.Properties

import com.hankl.spark.test.SecondSort
import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory




/**
  * Created by hankl on 2018/3/14.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val log=LoggerFactory.getLogger(WordCount.getClass)
    Logger.getLogger("com.coocaa.spark.WordCount").setLevel(Level.ERROR)
   val conf= new SparkConf().setAppName("aaa").setMaster("spark://192.168.1.50:8080").set("spark.driver.host","172.20.142.42")
    val sc=new SparkContext(conf)
   /* val textRDD=sc.textFile("D:\\test.txt")
    val resultRDD=textRDD.flatMap(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_ + _)
    println(resultRDD.collect())*/

  /*  val str = sc.getLocalProperty("spark.defniation.key")
    println(str)*/


//    val digest=DigestUtils.md5Hex("hankl")
//    println(digest)







   /* val pro=new Properties()
    val stream = WordCount.getClass.getClassLoader.getResourceAsStream("agrs.properties")
    pro.load(stream)
    val value = pro.get("spark.defniation.key")
    println(value)*/


   /*val properties = new Properties();
    import java.io.BufferedReader
    val bf=new BufferedReader(new FileReader("src/agrs.properties"))
//    agrs.properties
//    D:\WORKSPACE\SparkTests\src\main\resources\agrs.properties
    properties.load(bf)
    val key=properties.get("spark.defniation.key")
    println(key)*/







    /*val rdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\a.txt")
     rdd.map{
       line=>
         val a = line.split("")(0).toInt
         val b = line.split("")(1).toInt
         (new SecondSort(a,b),line)
     }.sortByKey().map(t=>t._2)
      .collect()*/
  }

}
