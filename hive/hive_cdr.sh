#!/bin/bash
datepart=$1
tps_pp_g_path=$2
tps_volte_base_extend_path=$3 
echo ${datepart}
echo ${tps_pp_g_path}
echo ${tps_volte_base_extend_path}
hive -hiveconf tps_volte_base_extend_path=${tps_volte_base_extend_path} -hiveconf tps_pp_g_path=${tps_pp_g_path} -f cdr_hql/create_table.q   
hive -S -e "alter table tps_volte_base_extend add partition(datepart='${datepart}') location '${tps_volte_base_extend_path}';"
hive -f cdr_hql/insert_tps_volte_building_abno_day.q
hive -f cdr_hql/insert_tps_volte_building_day.q
hive -f cdr_hql/insert_tps_volte_building_e_day.q
hive -f cdr_hql/insert_tps_volte_e_day.q
# pass 3 args 
# sh hive_cdr_args.sh 20170620 /user/boco/cdr/input/ppg/ /user/boco/cdr/output/base/20170620/tps_volte_base_extend 

