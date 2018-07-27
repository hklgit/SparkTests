#!/bin/bash

basedir=$(cd $(dirname $0);pwd)

###############################
#  主方法
###############################
main(){
  #执行的程序可以是以下形式中的一种: <process_type>.sh <process_conf_name>
  #1.Hive程序
  sh ${basedir}/comm/hive_ddl.sh "$@"

}

main "$@"
