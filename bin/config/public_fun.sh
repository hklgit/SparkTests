################# 公共方法  作者：马全辉 (转载请注明出处)#############################
#ftp拉取数据写入hive的校验
#第一个参数hdfs的目录第二个参数是hive查询表的数量
function ftp_to_hive_check(){
    ftpnum=`hadoop fs -cat $1|wc -l`
    ftpnum=`expr $ftpnum - 1`
    hivenum=`hive -e "$2"`
    if [ x"$ftpnum" = x"$hivenum" ] ;then
      echo "写入hive成功"
    else
      echo "写入hive数量不匹配"
      exit 1
    fi
}
#ftp写入hdfs
#第一个参数是ftp的地址，第二个参数对hdfs的目录地址
function ftp_to_hdfs(){
     j=0
     while [ $j -lt 10 ]
     do
	    hadoop distcp -m 2 $1  $2
	    if [ $? -eq 0 ] ;then 
	     break
	    else
	       echo "同步ftp文件失败"
	       exit 1
	    fi
    let j++
    done
}
#hdfs中csv文件解析到hive中
#第一个参数是时间，第二个参数库名，第三个参数是表名，第四个参数是  hive表的前缀,第五个参数是csv文件分隔符,第六个参数是hive表是否是分区,第七个参数是分区字段
#注意:num-executors=1这个数量千万不可以改 
function hdfs_to_hive(){
str=''
    if [ x"$4" = x"" ] ;then 
       str=''
     else
       str=$4
    fi
echo ${table_list[$i]}
run_cmd=" --master yarn-client --conf spark.input.date=$1 \
--driver-memory=1g \
--executor-memory=3g \
--num-executors=1 \
--class com.coocaa.spark.data.usrdb.app.HdfsDataToHiveByDelimiterNew ${base_dir}/data_app-0.0.1.jar $2 $3 ${str} $5 $6 $7"
 echo ${run_cmd}
 spark-submit ${run_cmd}
 	    if [ $? -ne 0 ] ;then 
 	     echo "解析csv文件出问题"
	     exit 1
	    fi
}

#hive表删除分区
#第一个参数是表hdfs的位置,第二个参数是多少天的分区,第三个参数是要删除分区的表
function delete_hive_partition(){
	 hdfs dfs -rm -r $1/.hive-staging_hive_*
	 hive -e "alter table $3 DROP IF EXISTS PARTITION (day='$2')"
	 echo "删除"$2"天的分区"
}
#csv文件
#csv文件导入mysql
#删除csv文件
#第一个参数是本地csv文件路径
function delete_local_csv(){
	file=$1
	rm -f $file	
}
#增量csv到mysql
#第一个参数是csv本地路径,第二个参数库名第三个参数表名第四个参数是mysqlIP第五个参数用户名第六个参数是密码第七个参数端口第八个参数表示是否是增量1全量2增量
function csv_to_mysql(){
    #删除第一行列名
	sed -i 1d $1
	if [ $? -eq 0 ] ;then
		sql="TRUNCATE TABLE $2.$3;LOAD DATA LOCAL INFILE '$1' INTO TABLE $2.$3 CHARACTER SET utf8;"
	else
		sql="LOAD DATA LOCAL INFILE '$1' INTO TABLE $2.$3 CHARACTER SET utf8;"
	fi
	mysql -h$4 -u$5 -p$6 -P$7  -e "${sql}"
}

#mysql增加字段，sqoop同步的时候要注意字段的顺序除非指定具体的字段名
#mysql全量导入hive分区表中(注意:表使用默认建表方式)
#第一个参数是mysqlIP第二个参数是端口第三个参数是数据库名第四个参数是表名第五个参数是hive的库名第六个参数是hive的表名第七个参数是分区的字段名第八个参数是分区时间
function sqoop_mysql_to_hive(){
	 sqoop import --connect jdbc:mysql://$1:$2/$3 --username hadoop2 --password pw.mDFL1ap --table $4 --hive-import --hive-overwrite --hive-table $5.$6 --hive-partition-key $7 --hive-partition-value $8 --hive-drop-import-delims --fields-terminated-by "\0001"  --lines-terminated-by "\n"
}

#hdfs目录csv文件下载到本地 
#第一个参数是hdfs目录第二个参数是本地路径
function hdfs_csv_local(){
  hadoop fs -get $1 $2
}
#从远程mysql获取数据到本地
#第一个参数是mysqlIP第二个参数用户名第三个参数是密码第四个参数端口第五个参数是sql语句第六个参数是本地路径
function mysql_to_local(){
  mysql -h$1 -u$2 -p$3 -P$4 --default-character-set=utf8 -e '$5' > $6
}

#
function azkaban_info_fun(){
result=`python $3/bin/config/azkabaninfo.py $1 $2`
OLD_IFS="$IFS" 
IFS="," 
arr=($result) 
IFS="$OLD_IFS" 
status=${arr[0]}
stateTime=${arr[1]}
}

#第一个参数是开始时间，第二个参数是结束时间，第三个参数是执行的数据格式，第四个参数是按天，周，月标识，第五个参数是执行的命令 
function execution_fun(){
if [ $# -eq 3 ]; then
format=$1
flag=$2
cmd=$3
nowday=`date -d now +%Y%m%d`
time=`date -d "yesterday" +%Y%m%d`
times=`date -d "yesterday" +%Y%m%d`
fi
if [ $# -eq 4 ]; then
format=$2
flag=$3
cmd=$4
nowday=`date -d now +%Y%m%d`
time=$1
times=$1
fi
if [ $# -eq 5 ]; then
format=$3
flag=$4
cmd=$5
nowday=$2
time=$1
times=$1
fi
nowday=`date -d $nowday +%Y%m%d`
if [ x"$flag" = x"3" ]; then
nowday=`date -d $nowday +%Y%m`
if [ x"$format" = x"1" ]; then
time=`date -d $time +%Y-%m`
times=`date -d $times +%Y-%m`
else
time=`date -d $time +%Y%m`
times=`date -d $times +%Y%m`
fi
fi

while [[ $time < $nowday ]]; do 
#天
if [[ x"$flag" = x"1" && x"$format" = x"1" ]]; then
times=$(date -d $times +%F)
echo "执行时间1:"$times
eval ${cmd//paramday/$times} 
result=$? 
times=$(date -d"$times 1 day" +%F)
time=$(date -d"$time 1 day" +%Y%m%d)
fi
#天
if [[ x"$flag" = x"1" && x"$format" = x"2" ]]; then
times=$(date -d "$times" +%Y%m%d)
echo "执行时间2:"$times
eval ${cmd//paramday/$times} 
result=$? 
times=$(date -d"$times 1 day" +%Y%m%d)
time=$(date -d"$time 1 day" +%Y%m%d)
fi
#周
if [[ x"$flag" = x"2" && x"$format" = x"1" ]]; then
times=$(date -d $times +%F)
echo "执行时间3:"$times
eval ${cmd//paramday/$times} 
result=$? 
times=$(date -d"$times 7 day" +%F)
time=$(date -d"$time 7 day" +%Y%m%d)
fi
#周
if [[ x"$flag" = x"2" && x"$format" = x"2" ]]; then
times=$(date -d "$times" +%Y%m%d)
echo "执行时间4:"$times
eval ${cmd//paramday/$times} 
result=$? 
times=$(date -d"$times 7 day" +%Y%m%d)
time=$(date -d"$time 7 day" +%Y%m%d)
fi
#月
if [[ x"$flag" = x"3" && x"$format" = x"1" ]]; then
times=$(date -d $times"-01 00:00:00" +%Y-%m)
echo "执行时间5:"$times
eval ${cmd//paramday/$times}  
result=$? 
time1=$times"-01 00:00:00"
times=$(date -d"$time1 next month" +%Y-%m)
time=$(date -d"$time1 next month" +%Y%m)
fi
#月
if [[ x"$flag" = x"3" && x"$format" = x"2" ]]; then
times=$(date -d $times"01 00:00:00" +%Y%m)
echo "执行时间6:"$times
eval ${cmd//paramday/$times}  
result=$? 
time1=$times"01 00:00:00"
times=$(date -d"$time1 next month" +%Y%m)
time=$(date -d"$time1 next month" +%Y%m)
fi

if [ x"$result" != x"0" ]; then
 echo "执行失败"
 exit 1
fi

done
}





function execution_fun_new(){


if [ $# -eq 3 ]; then
format=$1
flag=$2
cmd=$3
nowday=`date -d now +%Y%m%d`
time=`date -d "yesterday" +%Y%m%d`
times=`date -d "yesterday" +%Y%m%d`
fi
if [ $# -eq 4 ]; then
format=$2
flag=$3
cmd=$4
nowday=`date -d now +%Y%m%d`
time=$1
times=$1
fi
if [ $# -eq 5 ]; then
format=$3
flag=$4
cmd=$5
nowday=$2
time=$1
times=$1
fi
nowday=`date -d $nowday +%Y%m%d`
if [ x"$flag" = x"3" ]; then
nowday=`date -d $nowday +%Y%m`
if [ x"$format" = x"1" ]; then
time=`date -d $time +%Y-%m`
times=`date -d $times +%Y-%m`
else
time=`date -d $time +%Y%m`
times=`date -d $times +%Y%m`
fi
fi

while [[ $time < $nowday ]]; do 
#天
if [[ x"$flag" = x"1" && x"$format" = x"1" ]]; then
times=$(date -d $times +%F)
echo "执行时间1:"$times
execute_time=times
execute_funs
result=$? 
times=$(date -d"$times 1 day" +%F)
time=$(date -d"$time 1 day" +%Y%m%d)
fi
#天
if [[ x"$flag" = x"1" && x"$format" = x"2" ]]; then
times=$(date -d "$times" +%Y%m%d)
echo "执行时间2:"$times
execute_time=${times}
execute_funs
result=$? 
times=$(date -d"$times 1 day" +%Y%m%d)
time=$(date -d"$time 1 day" +%Y%m%d)
fi
#周
if [[ x"$flag" = x"2" && x"$format" = x"1" ]]; then
times=$(date -d $times +%F)
echo "执行时间3:"$times
execute_time=times
execute_funs
result=$? 
times=$(date -d"$times 7 day" +%F)
time=$(date -d"$time 7 day" +%Y%m%d)
fi
#周
if [[ x"$flag" = x"2" && x"$format" = x"2" ]]; then
times=$(date -d "$times" +%Y%m%d)
echo "执行时间4:"$times
execute_time=times
execute_funs
result=$? 
times=$(date -d"$times 7 day" +%Y%m%d)
time=$(date -d"$time 7 day" +%Y%m%d)
fi
#月
if [[ x"$flag" = x"3" && x"$format" = x"1" ]]; then
times=$(date -d $times"-01 00:00:00" +%Y-%m)
echo "执行时间5:"$times
execute_time=times
execute_funs
result=$? 
time1=$times"-01 00:00:00"
times=$(date -d"$time1 next month" +%Y-%m)
time=$(date -d"$time1 next month" +%Y%m)
fi
#月
if [[ x"$flag" = x"3" && x"$format" = x"2" ]]; then
times=$(date -d $times"01 00:00:00" +%Y%m)
echo "执行时间6:"$times
execute_time=times
execute_funs
result=$? 
time1=$times"01 00:00:00"
times=$(date -d"$time1 next month" +%Y%m)
time=$(date -d"$time1 next month" +%Y%m)
fi

if [ x"$result" != x"0" ]; then
 echo "执行失败"
 exit 1
fi

done
}