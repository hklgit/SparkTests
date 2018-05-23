//package com.coocaa.spark
//
//import scalikejdbc.{DB, SQL}
//import scalikejdbc.config.DBs
//
///**
//  * Created by hankl on 2018/4/25.
//  */
//
//
//case class t1(id:Int,name:String)
//object ScalikeJDBCTest {
//
//
//  def main(args: Array[String]): Unit = {
//
//    //解析application.conf的文件。
//    DBs.setup()
//
//    //DBs.setupAll()
//    val i = DB.autoCommit { implicit session =>
//      SQL("insert into t1(id,name) values(?,?)").bind(88, "lisi").update().apply()
//    }
//
//
//    println(i)
//  }
//
//
//
//
//
//
//
//
//
//
//
//}
