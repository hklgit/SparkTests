#!/bin/bash

###############################################################
#  此处为spark online an offline 项目静态常量配置，如需要修改，请发邮件联系jiaowenbin@coocaa.com
###############################################################

#开发者名字  主要用于job_name 及 事故和数据疑问等 联系
SPARK_ONLINE_AND_OFFLIE_DEVELOPER_NAME="jiaowenbin"

yarn_rest_url='http://xl.namenode2.coocaa.com:8088/cluster/scheduler'
yarn_rest_url_back='http://xl.namenode1.coocaa.com:8088/cluster/scheduler'
FTP_URL=114.215.168.62
FTP_USER=big_data
FTP_PASSWORD=H6ogVqHGxmJ
function check_status(){
appname=$1
	status=`curl  ${yarn_rest_url} |fgrep $appname --color |head -n1 |awk -F, '{print $9}'`
status_back=`curl  ${yarn_rest_url_back} |fgrep $appname --color |head -n1 |awk -F, '{print $9}'`
status=$status''$status_back
echo ${status//\"/}
}

###############################################################
#  检测hdfs目录是否存在以及目录大小情况，0=存在且大于0 、1=
###############################################################
function check_hdfs_filePahtOrData(){
 hdfs dfs -test -d ${outputPath}
  if [ $? -ne 0 ] ; then
    return 1
 fi
 fileSize=$(hdfs dfs -du -s ${outputPath} | awk '{print $1}')
 if [ "${fileSize}" == "0" ] ; then
    return 1
 fi
 return 0
}