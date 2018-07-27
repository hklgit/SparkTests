#!/bin/bash
#
# Hive 相关公用方法 
#######################################
##### hive 执行函数
##### Author:Chengyeliang
##### e.g.  hive_exec {hql}}
#######################################
function hive_exec(){
    #打印执行的sql
    echo "hive_exec :$1"
    $HIVE_BIN_EXEC -e "$1" 
    if [ $? -ne 0 ] ; then            
        return -1
    fi 
}

#######################################
##### 重试hive操作，仅限轻量hql 如DDL操作
##### Author:Chengyeliang
##### 注意：加分区的sql 字符串传参 以引号方式引起来
##### retry_hql "${hql}"  "add_partition failed"
##### e.g.  retry_hql {hql} {error message name}
#######################################
function retry_hql(){

    for (( i = 1; i <= ${RETRY_NUM}; i++ )); do
        echo "hive_exec" && hive_exec "$1"
        if [ $? -eq 0 ] ; then
            break
        elif [[ $i -eq ${RETRY_NUM} ]]; then
            error_log "$2 "
        fi
        sleep 30
    done

}

#######################################
##### hive 加分区函数 (不带重试，不推荐)
##### Author:Chengyeliang
##### e.g.  RetryDo {DbName} {TableName} {PartitionName} {Date} {LocationPath}
#######################################
function add_partition(){
    for ii in $1
    do
        useQl="USE ${ii}"
        hive_exec "${useQl};ALTER TABLE $2 DROP IF EXISTS PARTITION($3='$4')"
        hive_exec "${useQl};ALTER TABLE $2 ADD PARTITION($3='${4}') LOCATION '$5'"
        if [ $? -ne 0 ] ; then            
            return -1
        fi       
    done
}

#hive_calc "conf_name"  
function hive_calc(){
  if [ x"${sql}" == "xdo_nothing" ];then
    error_log "HIVE DO_NOTHING"
  else
    if [ "$mysql_tablename" != "" ] ;then 
      echo "tablename=$mysql_tablename"
      hive -e "${sql}" > $mysql_tablename 
    else
       hive -e "${sql}" 
    fi
    
    if [ $? -ne 0 ];then
      error_log "HIVE failed"
      exit ${EXE_FAILED}
    fi
    echo "table_location=$table_location"
    if [ x"${table_location}" != "x" ];then
      hadoop fs -test  -e $table_location
      if [ $? -ne 0 ];then
        error_log "HIVE failed,目标表不存在!"
        exit ${EXE_FAILED}
       else
        echo "测试通过!"
    fi
   fi
  fi
  
  # 导入mysql
  if [ "x${tomysql}" != "x" ];then
    $tomysql
    if [ $? -ne 0 ];then
      error_log "导入Mysql 数据库失败"
      exit ${EXE_FAILED}
    fi
  fi
  
  # 导入mysql
  if [ "x${exec_sql}" != "x" ];then
    #mysql --host=$mysql_host --port=$mysql_port --user=$mysql_user --password=$mysql_passwd --database=$mysql_db -A -e "delete from ${mysql_tablename} where create_date='$day'"
    mysql --host=$mysql_host --port=$mysql_port --user=$mysql_user --password=$mysql_passwd --database=$mysql_db -A -e "${exec_sql}"
    if [ $? -ne 0 ];then
      error_log "导入Mysql 数据库失败"
      exit ${EXE_FAILED}
    fi
  fi

 
  # 导入mysql
  if [ "x${create_done}" != "x" ];then
 create_done ${create_done}
 fi
}

