#!/bin/bash

basedir=$(dirname $0)/../../
echo "hive path=${basedir}"
. ${basedir}/bin/config/date_param.sh
. ${basedir}/bin/config/const.sh
. ${basedir}/bin/utils/hive_function.sh
main(){
 conf_name=${hive_conf}
source $basedir/conf/hive/${conf_name}.conf
	    #if [ x"${target_file_list}" != "xdo_nothing" ];then
	      #   checkData 
	    #fi
  hive_calc ${conf_name}
}
main "$@"