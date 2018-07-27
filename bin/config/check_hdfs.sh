#!/bin/bash

###############################################################
#  检查hdfs文件目录是否存在及文件是否为空
###############################################################

hdfs_flag=0
hdfs_null_flag=0
#  检查hdfs文件目录是否存在及文件是否为空
function check_hdfs_fun(){
    hadoop fs -test -e  $1
	if [ $? -eq 0 ] ;then  
	    hdfsnums=`hadoop fs -dus $1 | awk '{print $1}'`  #文件的大小
	    	if [ $hdfsnums -gt 0 ] ;then  
			    hdfs_flag=1
			    else 
			    hdfs_flag=0
			fi    
	else  
	    hdfs_flag=0  
	fi
}
#  检查hdfs文件目录是否存在
function check_hdfs_null_fun(){
    hadoop fs -test -e  $1
	if [ $? -eq 0 ] ;then 
	 hdfs_null_flag=1
	fi
}
#使用例子check_hdfs_fun /apps/external/hive/base_all_mac/day=2017-07-30
#echo $hdfs_flag
#echo $hdfs_null_flag