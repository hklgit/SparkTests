#!/bin/bash
#author hkl
base_dir=$(cd `dirname ../../`; pwd)
echo "work directory:"$base_dir
#导入通用配置项和需要的函数库#
source $base_dir'/bin/config/etl_const.sh'
source $base_dir'/bin/config/const.sh'
source $base_dir'/bin/utils/hadoop_function.sh'
source $base_dir'/bin/utils/common_function.sh'
source $base_dir'/bin/config/date_param.sh'
source $base_dir'/bin/config/check_hdfs.sh'


run_cmd=" --master yarn-client \
          --driver-memory 1g \
          --executor-memory=3g \
          --num-executors=20 \
          --class kafka.KafkaTest ${base_dir}/SparkTests-1.0-SNAPSHOT.jar "

#main函数入口#为了解决一个问题：就是数据没有插入但是任务依旧会报告说success.
#1:如果主程序没有执行成功那么不会删除之前的备份数据
#2:检测是否有对应的数据文件生成，如果没有的话也不会执行删除操作，并且要给出提示。
main(){
 echo ${run_cmd}
 /usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd}
}
main $@






#main(){
# echo ${run_cmd}
# spark-submit ${run_cmd}
# if [ $? -eq 0 ] ; then
#  echo "任务执行出现错误。。。"
# fi
# date=`date --date "${day}  3 days ago"  '+%Y-%m-%d'`
# echo ${date}
# hive -e "alter table edw.user_play_history_bg DROP IF EXISTS PARTITION (day='${date}')"
# hdfs dfs -test -d /apps/hive/warehouse/edw.db/user_play_history_bg/day=${date}
# if [ $? -eq 0 ] ; then
#	hdfs dfs -rm -r /apps/hive/warehouse/edw.db/user_play_history_bg/day=${date}
# fi
#}
#main $@


