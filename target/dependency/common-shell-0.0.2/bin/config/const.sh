#!/bin/bash

###############################################################
#  此处为dw-etl静态常量配置，如需要修改，请发邮件联系jiaowenbin@coocaa.com
###############################################################



#hadoop mr运行时公共参数#
SCHEDULER_POOL="anl"
COMPRESSION_MAP_OUTPUT="true"
LINE_RECODRDREADER_MAXLENGTH="1024*1024"
COMPRESSION_CODEC_GZIP="org.apache.hadoop.io.compress.GzipCodec"
COMPRESSION_CODEC_LZOP="org.anarres.lzo.hadoop.codec.LzopCodec"
#hadoop env end



#程序配置
#重试操作的重试次数
RETRY_NUM=3
#done服务等待重试 次数 
DONE_RETRY_NUM=72

#自定义报警的开关 1：打开 ；非1：关闭
IS_OPEN_ALARM=1



#exit status
EXE_FAILED=1

#用户tmp路径配置
user_tmp_path="/home/$(whoami)/log"


#编码设置 start
export LANG="zh_CN.UTF-8"
export LC_CTYPE="zh_CN.UTF-8"
export LC_NUMERIC="zh_CN.UTF-8"
export LC_TIME="zh_CN.UTF-8"
export LC_COLLATE="zh_CN.UTF-8"
export LC_MONETARY="zh_CN.UTF-8"
export LC_MESSAGES="zh_CN.UTF-8"
export LC_PAPER="zh_CN.UTF-8"
export LC_NAME="zh_CN.UTF-8"
export LC_ADDRESS="zh_CN.UTF-8"
export LC_TELEPHONE="zh_CN.UTF-8"
export LC_MEASUREMENT="zh_CN.UTF-8"
export LC_IDENTIFICATION="zh_CN.UTF-8"
#编码设置 end
