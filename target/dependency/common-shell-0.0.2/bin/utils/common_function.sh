#!/bin/bash


##########抽象公共方法################


#######################################
##### 重试函数
##### Author:Chengyeliang
##### e.g.  retry_do{action}
####################################### 

function retry_do(){

    originArgs=$@
    #action参数
    newArgs=${originArgs//$1/}
    for (( i = 1; i <= ${RETRY_NUM}; i++ )); do
        echo $1 && $($1 "$newArgs")
        if [ $? -eq 0 ] ; then
            break
        elif [[ $i -eq ${RETRY_NUM} ]]; then
            error_log "$1 $2 $3 failed"
        fi
    done
}

########################################################################
##### 发送报警 (仅用于需要自定义报警的情况，其余情况交给azkaban报警维护)
##### Author:chengyeliang
##### e.g.  send_alarm "$(date +'%F %T') [error] 小伙XX出错了" "" 
########################################################################
# send_alarm "subject" "content" "group"
function send_alarm() {
    subject="$1"
    subject=$(echo "$subject" | iconv -f utf8 -t gbk)
    content=$(echo "$2" | iconv -f utf8 -t gbk)
    ALARM_GROUP="$3"
    curl -d "subject=$subject&content=$content"  "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=$ALARM_GROUP"
}


########################################################################
##### 输出error log日志，并且发送错误报警
##### Author:Chengyeliang
##### e.g.  error_log {error_message}}
########################################################################

function error_log() {
    echo "$(date +'%F %T') [error] $1"
    if [ ${IS_OPEN_ALARM} -eq 1 ] ; then
        send_alarm "$(date +'%F %T') [error] $1" "" "${ALARM_GROUP}"
    fi
    exit ${EXE_FAILED}
}

########################################################################
##### 说明：不建议在外部用
##### Author:created by lishuai 2016-03-11,
#####        update  by chengyeliang 2016-03-14
##### e.g.  
########################################################################
# wait_done "done_url" "done_target" "done_suffix"
function request_done(){
    done_url=$1
    echo "request done service $done_url"
    times=0
    while true
        do
            status=`curl $done_url`
            if [ $status -eq $DONE_STATUS_SUCC ] ; then
                echo "done服务请求$done_url成功，后续可以开搞"
                break ;
            elif [ $status -eq $DONE_STATUS_EXCEPTION ] ; then
                if [[ "$times" -gt ${RETRY_NUM} ]] ; then
                    echo "done服务异常，重试超过等待次数(3)，直接退出"
                    error_log "$0 wait_done times over ${times}" 
                    break ;
                fi
            else 
                if [[ "$times" -gt ${DONE_RETRY_NUM} ]] ; then
                    echo "done标识 $done_url 还未就绪，重试超过等待次数(默认6小时)，直接退出"
                    error_log "$0 wait_done times over ${times}" 
                    break ;
                fi 
                echo "done标识 $done_url 还未就绪,休息5分钟，我们再来"               
            fi
            times=$((times+1))
            echo "当前重试次数:$times"
            sleep $((5*60))
        done
}

########################################################################
##### done服务：创建 done标识,仅需一个参数done_name(带日期)
##### Author:Chengyeliang
##### e.g.  create_done {done_name}
##### e.g.  check_done edw_platform_pub_click_detail_2016-03-15
########################################################################
create_done(){
    done_name=$1
    request_done $DONE_DELETE_URL$DONE_CATEGORY$done_name 
    request_done $DONE_CREATE_URL$DONE_CATEGORY$done_name
}

########################################################################
##### done服务：check done标识,仅需一个参数done_name(带日期)
##### Author:Chengyeliang
##### e.g.  check_done {done_name}
##### e.g.  check_done edw_platform_pub_click_detail_2016-03-15
########################################################################
check_done(){
    done_name=$1
    request_done $DONE_RETRIEVES_URL$DONE_CATEGORY$done_name
}



########################################################################
##### 
##### Author:lishuai
##### e.g.  
########################################################################
#check_fun check_fun "cmd" 
check_fun(){
cmd=$1
 while true
        do
        $(${cmd})
        if [ $? -eq 0 ] ; then
             echo "命令执行成功...."
             break ;
        else
             #休息五分钟
             echo "休息1分钟，重新尝试"
            if [ $times -gt 36 ] ; then
                exit 1
            fi
            times=$((times+1))
            echo "当前重试次数:$times"
            sleep 20
        fi
      done
}


##################################################old函数 start###################################################

#######################分割线  下边为old函数,当发现有需求的时候，请完善old函数并且提到分割线上边##################
function getIp() {
    echo $(/sbin/ifconfig | grep Mask | awk '{print $2}' | sed 's/addr://' | grep -v '127.0.0.1' | head -n 1)
}

function mkDir() {
    if [ ! -d $1 ] ; then
        mkdir $1
    fi
}

# info "example text"
function infoM() {
    echo "$(date +'%F %T') [info] $1"
}

# succ "example text"
function succM() {
    echo "$(date +'%F %T') [succ] $1"
    if [ ${IS_OPEN_ALARM} -eq 1 ] ; then
        sendEmail "${SEND_EMAIL_FROM}" "${SEND_EMAIL_TO_SUCC}" "$(date +'%F %T') [succ] $1" "$(date +'%F %T') [succ] $1"
    fi
}

# error "example text"
function errorM() {
    echo "$(date +'%F %T') [error] $(getIp) $1"
    if [ ${IS_OPEN_ALARM} -eq 1 ] ; then
        send_alarm "$(date +'%F %T') [error] $1" "" "${ALARM_GROUP}"
    fi
    #exit 1
}

# sendEmail "from" "to" "subject" "body"
function sendEmail() {
    from="$1"
    to="$2"
    subject="$3"
    body="$4"
    (echo "Subject: ${subject}" 
    echo "MIME-Version: 1.0" 
    echo "Content-Type: text/html" 
    echo "Content-Disposition: inline"
    echo "From: ${from}"
    echo "To: ${to}"
    echo "${body}") | /usr/sbin/sendmail ${to}

}


#检测MR的输出(_SUCCESS/输出目录/文件大小,路径以/结尾)
function checkMR(){
    outputPath=$1
    $HADOOP_BIN dfs -test -d "${outputPath}"
    if [ $? -gt 0 ] ; then
        errorM "${outputPath} not exist"
    fi
    $HADOOP_BIN dfs -test -e "${outputPath}_SUCCESS"
    if [ $? -gt 0 ] ; then
        errorM "${outputPath}_SUCCESS not exist"
    fi
    fileSize=$($HADOOP_BIN dfs -dus -h ${outputPath} | awk '{print $2}')
    if [ "${fileSize}" == "0" ] ; then
        errorM "${outputPath} empty"
    fi
    echo "succ"
}
function checkHdfs(){
    outputPath=$1
    $HADOOP_BIN dfs -test -d "${outputPath}"
    if [ $? -gt 0 ] ; then
        echo "${outputPath} not exist"
        exit -1
    fi
    fileSize=$($HADOOP_BIN dfs -dus -h ${outputPath} | awk '{print $2}')
    if [ "${fileSize}" == "0" ] ; then
        echo "${outputPath} empty"
        exit -1
    fi
    echo "succ"
}


####################################!/bin/bash

#公共方法

#发短信 sendsms 10086 送点话费
#function sendsms(){
#    smsToken='676698b7dacdbd5d969092e875772a9f';
#    smsSource='dianjing';
#    smsRoleName='sms_mc';
#    smsApi='http://mc.ad.360.cn/api.php';
#    smsSign=`echo -e -n "${smsToken}|${smsRoleName}" | md5sum |awk '{print $1}'`;
#    #smsSign="ea2ae0bf40debf00b76b2fe776b06559";
#    curl -d "source=dianjing&sign=${smsSign}&roleName=${smsRoleName}&smsMobile=$1&smsContent=$2"  $smsApi;
#}

function sendsms(){
    curl -d "subject=${1}"  "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=test_gehuanyun"
}


#add by chengyeliang
function send_to_me_and_echo(){
    if [[ $1 =~ "succ" ]]
    then
        echo $1
        return 0
    fi
    log=$(get_current_date)
    ip=$(getIp)
    log="{${log}}{${ip}}{import hive}{${1}}"
    sendsms "${log}"
    echo $1
}


#取时间戳
function get_current_timestamp(){
    echo $(date +%s)
}

#取当前日期
function get_current_date(){
    echo $(date +%Y-%m-%d_%H:%M:%S)
}


#日期检测
function check_date(){
    local date_param
    if [ $1 ] && [ 10 -eq `expr length $1` ] ; then
        date_param=$1
    else	
        date_param=`date +%Y-%m-%d -d -1days`
    fi
    echo $date_param
}

#创建目录
function create_dir(){
    if [ ! -d $1 ] ; then
        mkdir -p $1
    fi
	if [ $? -ne 0 ] ; then
	    send_to_me "创建目录失败:$1"
	    echo "创建目录失败:$1"
        exit
	fi
}

#删除文件
function rmrf_file(){
    if [ -f $g_tmpfile ] ; then
        rm -rf $g_tmpfile
    fi
}

#清空指定的hadoop目录
function truncate_dir(){
    hadoop_dir=$1
    $HADOOP_BIN_EXEC dfs -test -d ${hadoop_dir}
    if [ $? == 0 ] ; then
        $HADOOP_BIN_EXEC dfs -rmr ${hadoop_dir}
    fi
}

#取ip
function getIp2(){
    echo $(/sbin/ifconfig  eth0 | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}')
}

#发送信息
function send_to_me(){
    log=$(get_current_date)
    ip=$(getIp)
    log="{${log}}{${ip}}{import hive}{${1}}"
    sendsms "${log}"
}


#记录失败日志 error_log {path} {filename} {info}
function fail_log(){
    log_path=$1
    if [ ! -d ${log_path} ] ; then
        mkdir ${log_path} && chmod -R 755 ${log_path}
    fi
    end=${log_path:(-1)}
    if [ "${end}" != "/" ] ; then
        log_path="${log_path}/"
    fi
    filename=$2
    log_info=$3
    current_date=$(get_current_date)
    fail_log_file="fail_log_"$(date +%Y%m%d)
    echo "[${current_date}] [${filename}] ${log_info}" >> ${log_path}${fail_log_file}
    send_to_me "${filename} ${log_info}"
    exit
}

#记录成功日志 succ_log {path} {filename} {info}
function succ_log(){
    log_path=$1
    if [ ! -d ${log_path} ] ; then
        mkdir ${log_path} && chmod -R 755 ${log_path}
    fi
    end=${log_path:(-1)}
    if [ "${end}" != "/" ] ; then
        log_path="${log_path}/"
    fi
    filename=$2
    log_info=$3
    current_date=$(get_current_date)
    succ_log_file="succ_log_"$(date +%Y%m%d)
    echo "[${current_date}] [${filename}] ${log_info}" >> ${log_path}${succ_log_file}
}

#获取基本文件名(点号之前)
function getBaseFileName(){
    echo ${1%.*}
}

#清空指定的hadoop目录
function truncateHdfsDir(){
    hadoop_dir=$1
    $HADOOP_BIN_EXEC dfs -test -d ${hadoop_dir}
    if [ $? == 0 ] ; then
        $HADOOP_BIN_EXEC dfs -rmr ${hadoop_dir}
    fi
}

#取当前日期
function getCurrentDate(){
    echo $(date +%Y-%m-%d_%H:%M:%S)
}

#取发短信需要的日期
function getSmsDate(){
    echo $(date +%m-%d_%H:%M:%S)
}

#取日志需要的日期
function getLogDate(){
    echo $(date +%H:%M:%S)
}

function getFirstDay(){
    echo $(date +%Y-%m-%d -d -1days)
}

function getSecondDay(){
    echo $(date +%Y-%m-%d -d -2days)
}

function getThirdDay(){
    echo $(date +%Y-%m-%d -d -3days)
}

#发送信息
function sendToMe(){
    ip=$(getIp)
    log="[${ip}]${1}"
    sendsms "${log}"
}

#取ip
function getIp(){
    echo $(/sbin/ifconfig  eth0 | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}')
}

#成功日志
function succLog(){
    scriptFileName=$(getBaseFileName $1)
    logInfo=$2
    __writeLog "Succ" "${scriptFileName}" "${logInfo}" "N"
}

#失败日志
function errorLog(){
    scriptFileName=$(getBaseFileName $1)
    logInfo=$2
    __writeLog "Error" "${scriptFileName}" "${logInfo}" "N"
}

#失败日志+退出
function errorLogExit(){
    scriptFileName=$(getBaseFileName $1)
    logInfo=$2
    __writeLog "Error" "${scriptFileName}" "${logInfo}" "Y"
}

#检测日志
function checkLog(){
    logInfo=$1
    __writeLog "Check" "Check" "${logInfo}" "N"   
}

#验证日志
function verifyLog(){
    logInfo=$1
    __writeLog "Verify" "Verify" "${logInfo}" "N"   
}

#记录日志 writeLog Error/Succ/Check ScriptFileName(去除扩展名) logInfo isExit
function __writeLog(){
    today=$(date +%Y%m%d)
    logPath="${COMMON_LOG_PATH}${today}"
    if [ ! -d ${logPath} ] ; then
        mkdir ${logPath} && chmod -R 755 ${logPath}
    fi
    logFileName="$1"
    scriptFileName="$2"
    logInfo="$3"
    isExit="$4"
    logDate=$(getLogDate)
    smsDate=$(getSmsDate)
    logContent=${logInfo/\#/}
    smsContent=${logInfo/\#/\\n}
    echo -e "[${logDate}][${scriptFileName}]${logContent}" >> ${logPath}/${logFileName}
    if [ "${logFileName}" == "Error" ] ; then
        sendToMe "[${smsDate}][${scriptFileName}]${smsContent}"
    fi
    if [ "${isExit}" == "Y" ] ; then
        exit
    fi
}
#####################################old 函数  end ######################################################

#########################如果有新增function ，请添加至old 函数 start之前#################################
