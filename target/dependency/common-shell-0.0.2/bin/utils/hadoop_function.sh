#!/bin/bash
#
########################################################################
##### hadoop 相关公用方法
##### Author:Chengyeliang
##### 说明：
########################################################################


########################################################################
##### 获取目录深度，返回目录深度值，不传参的目录深度为0
##### Author:Chengyeliang
##### e.g.  get_dir_depth {dir}
function get_dir_depth(){
    echo $(echo $1| tr -cd '/' | wc -c)
}

########################################################################
##### 清空hadoop 目录
##### Author:Chengyeliang
##### e.g.  remove_hadoop_dir {hadoop_dir}}
function remove_hadoop_dir(){
    hadoop_dir=$1
    $HADOOP_BIN_EXEC dfs -test -d ${hadoop_dir}
    if [ $? -eq 0 ] ; then
        $HADOOP_BIN_EXEC dfs -rmr ${hadoop_dir}
        if [ $? -ne 0 ] ; then
            exit ${EXE_FAILED}
        fi
    fi
}

########################################################################
##### 带有强验证的删除目录
##### 要求：目录深度达到六层；目录末尾以'%Y-%m-%d'时间结尾，否则报错退出
##### Author:Chengyeliang
##### e.g.  remove_hadoop_dir_safe {hadoop_dir}} 
#####  /home/hdp-guanggao/tmp/christmp/2015121803/aaa 失败：目录并非以时间结尾
#####  /2015121803/aaa目录不存在
#####  /home/hdp-guanggao/tmp/christmp 目录层数过低，删除失败退出
#####  /home/hdp-guanggao/ba_data/hive/edw_platform_pub_click_detail/dt=2016-03-18 删除成功
function remove_hadoop_dir_safe(){
    hadoop_dir=$1
    dir_depth=$(get_dir_depth $hadoop_dir)
    if [ ${dir_depth} -le 5 ] ; then
            error_log  "[$0]${hadoop_dir} 目录层数过低，禁止线上程序删除"
    fi    
    hadoop_dir_noline=${hadoop_dir////}
    dir_date=${hadoop_dir_noline:0-10:10}
    test_date=$(date -d "$dir_date" +'%Y-%m-%d' )
    if [ "${test_date}" != "${dir_date}" ] ; then
        arr=(${hadoop_dir//// })  
        parent_dir=${arr[${#arr[*]}-2]}
        parent_date=${parent_dir:0-10:10}
        test_parent=$(date -d "$parent_date" +'%Y-%m-%d')
        if [ "${test_parent}"x != "$parent_date"x ] ; then
            error_log  "[$0]${hadoop_dir} 目录及上一层均非时间结尾，禁止删除； error:$parent_date"
        fi
    fi
    $HADOOP_BIN_EXEC dfs -test -d $1
    if [ $? -eq 0 ] ; then
        $HADOOP_BIN_EXEC dfs -rmr $1
        if [ $? -ne 0 ] ; then
            error_log  "[$0]${hadoop_dir} 删除异常，退出"
        fi
        echo "$1 删除成功"
    else
        echo "$1 目录不存在"
    fi
}
########################################################################
##### 检测MR的输出 (_SUCCESS/输出目录/文件大小,路径以/结尾)
##### Author:Chengyeliang
##### e.g.  checkMR {hadoop_dir}
##### 
function check_mr(){
    outputPath=$1
    $HADOOP_BIN_EXEC dfs -test -d "${outputPath}"
    if [ $? -gt 0 ] ; then
        error_log "${outputPath} not exist"
    fi
    $HADOOP_BIN_EXEC dfs -test -e "${outputPath}_SUCCESS"
    if [ $? -gt 0 ] ; then
        error_log "${outputPath}_SUCCESS not exist"
    fi
    fileSize=$($HADOOP_BIN_EXEC dfs -dus -h ${outputPath} | awk '{print $2}')
    if [ "${fileSize}" == "0" ] ; then
        error_log "${outputPath} empty"
    fi
    echo "check_mr succ"
}

########################################################################
##### 检测hdfs的文件大小 
##### Author:Chengyeliang
##### e.g.  check_hdfs_data {hadoop_dir}
##### check_hdfs_data /home/hdp-guanggao/ba_data/hive/edw_platform_pub_click_detail/dt=2016-03-15
function check_hdfs_data(){
    outputPath=$1
    $HADOOP_BIN_EXEC dfs -test -d "${outputPath}"
    if [ $? -gt 0 ] ; then
        error_log "${outputPath} not exist"
    fi
    fileSize=$($HADOOP_BIN_EXEC dfs -dus -h ${outputPath} | awk '{print $2}')
    if [ "${fileSize}" == "0" ] ; then
        error_log "${outputPath} empty"
    fi
    echo "check_hdfs_data succ"
}



########################################################################
##### 
##### Author:lishuai
##### e.g.  
########################################################################
#mapred_cals "params" 执行hadoop jar
mapred_calc(){
run_cmd=$1
 if [ x"${run_cmd}" != "xdo_nothing" ];then
	    hadoop jar ${run_cmd} 
	      if [ $? -ne 0 ];then
		    echo "MapRed failed"
		    exit 1
          fi
 fi
}