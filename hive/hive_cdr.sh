#!/bin/bash
datepart="20170620"
tps_volte_base_extend_path="/user/boco/david/finger/output82/tps_volte_base_extend"
hive -hiveconf tps_volte_base_extend_path=${tps_volte_base_extend_path} -f cdr_hql/create_table.q   
hive -S -e "alter table tps_volte_base_extend add partition(datepart='${datepart}') location '${tps_volte_base_extend_path}';"
hive -f cdr_hql/insert_tps_volte_building_abno_day.q
hive -f cdr_hql/insert_tps_volte_building_day.q
hive -f cdr_hql/insert_tps_volte_building_e_day.q
hive -f cdr_hql/insert_tps_volte_e_day.q

