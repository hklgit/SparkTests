#!/bin/bash


######################################################
# 公共变量处理： 有问题找@李帅 @程叶椋
######################################################


#yyyy-mm-dd 昨天
day=$(date -d -1day +'%Y-%m-%d')
#yyyymmmdd  昨天
day_str=$(date -d -1day +'%Y%m%d')
#yyyy-mm-dd  今天
cur_day=`date --date='1 hour ago' +'%Y-%m-%d'`
#yyyymmdd 今天
cur_day_str=`date --date='1 hour ago' +'%Y%m%d'`
#0-23 当前小时
shour=`date --date='1 hour ago' +'%H'`
ehour=`date --date='1 hour ago' +'%H'`
#0-50 当前分钟 
minute=`date +'%M'`
hive_conf=""


########################################################
# 验证 时间参数
########################################################
function __check_args_date() {

    if [ "$1"x = "default_value"x ] ; then
        day=$(date -d -1day +'%Y-%m-%d')
        day_str=$(date -d -1day +'%Y%m%d')
    else
        date_param=$(date -d "$1" +'%Y-%m-%d')
        if [ "${date_param}" != "$1" ] ; then
            echo "$0 the param date illegal:$1" 
            exit ${EXE_FAILED}
        fi
        day=${date_param}
        day_str=$(date -d "${day}" +'%Y%m%d')
    fi 

}
function __check_args_nowdate() {

    if [ "$1"x = "default_value"x ] ; then
        day=$(date -d "now" +'%Y-%m-%d')
        day_str=$(date -d "now" +'%Y%m%d')
    else
        date_param=$(date -d "$1" +'%Y-%m-%d')
        if [ "${date_param}" != "$1" ] ; then
            echo "$0 the param date illegal:$1" 
            exit ${EXE_FAILED}
        fi
        day=${date_param}
        day_str=$(date -d "${day}" +'%Y%m%d')
    fi

}
########################################################
# 验证 时间参数
########################################################
function __check_args_hour() {
	hour=""
    if [ "$1"x = "default_value"x ] ; then
        hour=`date --date='1 hour ago' +'%H'`
	else
		hour=$1
    fi
    length=`expr length $hour`
    
	if [[ ${hour:0:1} -eq "0" ]] && [ ${length} == '2' ];then
		hour=${hour:1:1}
	fi
    echo $hour
}
function __check_value() {
    value=$2
	echo '参数为：'$1-'value为：'${value}
    if [[ "${value:0:1}"x = "-"x ]] || [[ "${value:0:1}"x = ""x ]]; then
            echo "$1参数为空，系统取默认值"
            OPTIND=$(($OPTIND-1))
            OPTARG="default_value"
    fi
}


function get_args() {
cmd="$0 "
if [[ $# -lt 1 ]];then  
    echo "USAGE:`basename $0` [-t value] [-c value] [-a value] [-s value] [-e value] [-u value] [-n value]"
    echo "-t date 时间"
    echo "-c conf 配置文件name"
    echo "-s 开始小时"
    echo "-e 结束小时"
    echo "-a user_arg 区分平台标示参数"
    echo "-u user_define 用户自定义参数"
    echo "-n 取今天的时间参数，而不是-t默认的昨天"
    exit ${EXE_FAILED} 
fi    
while getopts :t:c:a:s:e:u:n: opt; do
    case $opt in
    	n)
        __check_value "n" $OPTARG        
        __check_args_nowdate $OPTARG
        cmd=$cmd" -n $OPTARG"
        ;;
        t)
        __check_value "t" $OPTARG        
        __check_args_date $OPTARG 
        cmd=$cmd" -t $OPTARG" 
        ;;
        c)
        __check_value "c" $OPTARG
        hive_conf=$OPTARG
        cmd=$cmd" -c $OPTARG" 
        ;;
        u) 
        cmd=$cmd" -u  $OPTARG" 
        ;;
        s)
        __check_value "s" $OPTARG
        shour=`__check_args_hour $OPTARG`
        cmd=$cmd" -s  $OPTARG"
        ;;
        e)
        __check_value "e" $OPTARG
        ehour=`__check_args_hour $OPTARG`
        cmd=$cmd" -e $OPTARG"
        ;;
        a)
        __check_value "a" $OPTARG
        arg=$OPTARG
        cmd=$cmd" -a  $OPTARG" 
        ;;
        \?) 
        echo "$0 Invalid param" 
        ;;
    esac
done
echo $cmd
}

##获取参数

get_args $@

echo "hive_conf=${hive_conf}"
echo "day=$day"
echo "datestr=$day_str"


