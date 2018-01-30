USE ${database};
set hive.exec.compress.output=true;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.groupby.skewindata=true;
set hive.mapred.mode=nonstrict;
set mapreduce.job.reduces=15; 

INSERT OVERWRITE TABLE tps_g_week
PARTITION(week=${week})
SELECT
max(A.scan_start_time) as scan_start_time,
from_unixtime(unix_timestamp())  as  insert_time,
A.region_id as region_id,
max(A.region_name) as region_name,
max(A.city_id) as city_id,
max(A.city_name) as city_name, 
A.grid_id as grid_id, 
max(A.grid_leftbottom_longitude) as grid_leftbottom_longitude, 
max(A.grid_leftbottom_latitude) as grid_leftbottom_latitude, 
max(A.grid_lefttop_longitude) as grid_lefttop_longitude,   
max(A.grid_lefttop_latitude) as grid_lefttop_latitude,   
max(A.grid_righttop_longitude) as grid_righttop_longitude,  
max(A.grid_righttop_latitude) as grid_righttop_latitude,  
max(A.grid_rightbottom_longitude) as grid_rightbottom_longitude, 
max(A.grid_rightbottom_latitude) as grid_rightbottom_latitude,  
4 as scene_level ,
count(distinct imsi) as imsi_distinct_count,
count(case when B.ishigh_price=1 then B.ishigh_price else null end)  as  high_price_imei_count,
sum(A.xdr_count) as xdr_count,
sum(A.rsrp_sum) as rsrp_sum,
sum(A.rsrp_sample_count) as rsrp_sample_count,
sum(A.rsrp_weak) as rsrp_weak,
CASE WHEN SUM(A.RSRP_sample_count)=0 THEN 0 ELSE SUM(A.RSRP_SUM)/SUM(A.RSRP_sample_count) END AS scrsrp_avg,
CASE WHEN SUM(A.RSRP_sample_count)=0 THEN 0 ELSE SUM(A.Rsrp_weak)/SUM(A.RSRP_sample_count) END AS Rsrp_weak_rate,
SUM(A.UN_MAXRSRP_SUM) AS UN_MAXRSRP_SUM,
SUM(A.UN_MAXRSRP_sample_count) AS UN_MAXRSRP_sample_count,
SUM(UN_MAXRSRP_weak) AS UN_MAXRSRP_weak,
CASE WHEN SUM(A.UN_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.UN_MAXRSRP_SUM)/SUM(A.UN_MAXRSRP_sample_count) END AS un_rsrpavg,
CASE WHEN SUM(A.UN_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.UN_MAXRSRP_weak)/SUM(A.UN_MAXRSRP_sample_count) END AS un_maxrsrp_weak_rate,
SUM(A.TE_MAXRSRP_SUM) AS TE_MAXRSRP_SUM,
SUM(TE_MAXRSRP_sample_count) AS TE_MAXRSRP_sample_count,
SUM(TE_MAXRSRP_weak) AS TE_MAXRSRP_weak,
CASE WHEN SUM(A.TE_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.TE_MAXRSRP_SUM)/SUM(A.TE_MAXRSRP_sample_count) END AS te_rsrpavg,
CASE WHEN SUM(A.TE_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.TE_MAXRSRP_weak)/SUM(A.TE_MAXRSRP_sample_count) END AS te_maxrsrp_weak_rate,
SUM(A.UN_INTER_SCRSRP_SUM)  as  un_inter_scrsrp_sum,
CASE WHEN SUM(A.UN_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.UN_INTER_SCRSRP_SUM)/SUM(A.UN_MAXRSRP_sample_count) END AS un_inter_scrsrp_avg,
SUM(A.UN_INTER_SCRSRP_weak)  as  un_inter_scrsrp_weak,
CASE WHEN SUM(A.UN_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.UN_INTER_SCRSRP_weak)/SUM(A.UN_MAXRSRP_sample_count) END AS un_inter_scrsrp_weak_rate,
SUM(A.TE_INTER_SCRSRP_SUM)  as  te_inter_scrsrp_sum,
CASE WHEN SUM(A.TE_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.TE_INTER_SCRSRP_SUM)/SUM(A.TE_MAXRSRP_sample_count) END AS te_inter_scrsrp_avg,
SUM(A.TE_INTER_SCRSRP_weak)  as  te_inter_scrsrp_weak,
CASE WHEN SUM(A.TE_MAXRSRP_sample_count)=0 THEN 0 ELSE SUM(A.TE_INTER_SCRSRP_weak)/SUM(A.TE_MAXRSRP_sample_count) END AS te_inter_scrsrp_weak_rate,
SUM(A.ulsinr_sum) as ulsinr_sum,
sum(A.ulsinr_sample_count) as ulsinr_sample_count,
CASE WHEN SUM(A.ULSINR_sample_count)=0 THEN 0 ELSE SUM(A.ULSINR_SUM)/SUM(A.ULSINR_sample_count) END AS ulsinravg,
SUM(A.is_mod3) as is_mod3,
SUM(A.is_mod6) as is_mod6,
SUM(A.is_mod30) as is_mod30,
SUM(A.is_mod0) as is_mod0,
SUM(A.is_mod1) as is_mod1,
SUM(A.diff_6) as diff_6,
CASE WHEN SUM(A.diff_6)=0 THEN 0 ELSE SUM(A.is_mod3)/SUM(A.diff_6) END AS m3_interfere_intensity, 
CASE WHEN SUM(A.diff_6)=0 THEN 0 ELSE SUM(A.is_mod6)/SUM(A.diff_6) END AS m6_interfere_intensity, 
CASE WHEN SUM(A.diff_6)=0 THEN 0 ELSE SUM(A.is_mod30)/SUM(A.diff_6) END AS m30_interfere_intensity, 
CASE WHEN SUM(A.diff_6)=0 THEN 0 ELSE SUM(A.is_mod0)/SUM(A.diff_6) END AS m0_interfere_intensity, 
CASE WHEN SUM(A.diff_6)=0 THEN 0 ELSE SUM(A.is_mod1)/SUM(A.diff_6) END AS m1_interfere_intensity, 
sum(A.ul_data) as ul_data, 
sum(A.dl_data) as dl_data,
sum(A.ul_data_qci1) as ul_data_qci1,
sum(A.dl_data_qci1) as dl_data_qci1,
sum(A.ul_data_qci2) as ul_data_qci2,
sum(A.dl_data_qci2) as dl_data_qci2,
sum(A.context_succinitalsetup_volte) as context_succinitalsetup_volte,
sum(A.erab_nbrsuccestab_volte) as erab_nbrsuccestab_volte,
sum(A.erab_nbrattestab_volte) as erab_nbrattestab_volte,
CASE WHEN sum(A.erab_nbrattestab_volte)=0 THEN 0 ELSE sum(A.erab_nbrsuccestab_volte)/sum(A.erab_nbrattestab_volte) END AS ERAB_NbrSuccEstabRate_volte,
sum(A.rrc_attconnestab) as rrc_attconnestab,
sum(A.rrc_succconnestab) as rrc_succconnestab,
CASE WHEN sum(A.rrc_attconnestab)=0 THEN 0 ELSE sum(A.rrc_succconnestab)/sum(A.rrc_attconnestab) END AS rrc_succconnestab_rate,
(CASE WHEN SUM(A.ERAB_NbrAttEstab_volte)=0 THEN 0 ELSE SUM(A.ERAB_NbrSuccEstab_volte)/SUM(A.ERAB_NbrAttEstab_volte) END)*(CASE WHEN SUM(A.RRC_AttConnEstab)=0 THEN 0 ELSE SUM(A.RRC_SuccConnEstab)/SUM(A.RRC_AttConnEstab) END)  as  wireless_access_rate_volte,
sum(A.erab_nbrreqrelenb_volte) as erab_nbrreqrelenb_volte,
sum(A.erab_nbrreqrelenb_normal_volte) as erab_nbrreqrelenb_normal_volte,
sum(A.erab_hofail_volte) as erab_hofail_volte,
case when sum(A.erab_nbrsuccestab_volte)=0 then 0 else (sum(A.erab_nbrreqrelenb_volte)-sum(A.erab_nbrreqrelenb_volte)+sum(A.erab_hofail_volte))/sum(A.erab_nbrsuccestab_volte) end as erab_dropcall_rate_volte,
sum(A.ho_att_volte) as ho_att_volte,
sum(A.ho_succ_volte) as ho_succ_volte,
CASE WHEN sum(A.ho_att_volte)=0 THEN 0 ELSE sum(A.ho_succ_volte)/sum(A.ho_att_volte) END AS ho_succ_rate_volte,
sum(A.redirect_count) as redirect_count,
sum(A.context_succinitalsetup) as context_succinitalsetup,
sum(A.ho_in_succ) as ho_in_succ,
1-(sum(A.redirect_count)/(sum(A.CONTEXT_SuccInitalSetup) + sum(A.HO_IN_Succ)))  as  redirect_reside_rate,
sum(A.iratho_att_volte) as iratho_att_volte,
sum(A.iratho_out_att_volte) as iratho_out_att_volte,
sum(A.iratho_volte) as iratho_volte,
CASE WHEN sum(A.ho_att_volte)=0 THEN 0 ELSE sum(A.iratho_volte)/sum(A.ho_att_volte) END AS ho_rate_volte,
sum(A.context_attinitalsetup) as context_attinitalsetup,
sum(A.traffic_volte) as traffic_volte,
sum(A.rrc_congestion) as rrc_congestion,
sum(A.rrc_att) as rrc_att,
CASE WHEN sum(A.rrc_att)=0 THEN 0 ELSE sum(A.rrc_congestion)/sum(A.rrc_att) END AS rrc_congestion_rate,
sum(A.erab_congestion) as erab_congestion,
sum(A.erab_att) as erab_att,
CASE WHEN sum(A.erab_att)=0 THEN 0 ELSE sum(A.erab_congestion)/sum(A.erab_att) END AS erab_congestion_rate, 
sum(A.context_abnormalrel_volte) as context_abnormalrel_volte,
sum(A.context_release_volte) as context_release_volte,
CASE WHEN sum(A.context_release_volte)=0 THEN 0 ELSE sum(A.context_abnormalrel_volte)/sum(A.context_release_volte) END AS context_dropcall_rate_volte,
MAX(g2_g3_g4.G4_FLOW),
MAX(g4_volte.G4_FLOW_VOLTE),
MAX(g2_g3_g4.G3_FLOW),
MAX(g2_g3_g4.G2_FLOW),
MAX(g2_g3_g4.g4_flow/(g2_g3_g4.g4_flow + g2_g3_g4.g3_flow + g2_g3_g4.g2_flow)) as G4_FLOW_RATE,
MAX(g4_volte.g4_flow_volte/(g2_g3_g4.g4_flow + g2_g3_g4.g3_flow + g2_g3_g4.g2_flow)) as VOLTE_FLOW_RATE,
case when MAX(dim.coveragetype_desc)  in ("党政军机关" , "党政军宿舍" , "武警军区" , "星级酒店商业中心" , "写字楼" ,
 "企事业单位" , "会展中心" , "机场" , "火车站" , "长途汽车站" , "码头" , "高铁" , "普铁" , "地铁" , "高速公路" , "国道省道" ,
  "城区道路" , "郊区道路" , "航道" , "休闲娱乐场所" , "体育场馆" , "广场公园" , "风景区" , "医院" , "高校" 
  , "中小学" , "高层居民区" , "低层居民区" , "城中村" , "别墅群" , "工业园区" , "集贸市场" , "乡镇" ,
 "村庄" , "边境小区" , "沙漠戈壁" , "山农牧林" , "近水近海域" , "公墓" , "其他") then 1 else 0 end as special_scene_grid_flag,
sum(A.context_attrelenb) as context_attrelenb,
sum(A.context_attrelenb_normal) as   context_attrelenb_normal,
sum(A.context_nbrleft) as context_nbrleft,
case when (sum(A.context_succinitalsetup)+sum(A.context_nbrleft))=0  then 0 else (sum(A.context_attrelenb)-sum(A.context_attrelenb_normal))/(sum(A.context_succinitalsetup)+sum(A.context_nbrleft)) end as wireless_dropcall_rate,
sum(A.context_attrelenb)-sum(A.context_attrelenb_normal) as dropcall,
sum(A.erab_nbrsuccestab) as erab_nbrsuccestab,
sum(A.erab_nbrattestab)   as erab_nbrattestab,
case when sum(A.erab_nbrattestab)=0 then 0 else sum(A.erab_nbrsuccestab)/sum(A.erab_nbrattestab) end as erab_nbrsuccestabrate,
(CASE  WHEN  SUM(A.ERAB_NBRATTESTAB)=0  THEN  0  ELSE  SUM(A.ERAB_NBRSUCCESTAB)/SUM(A.ERAB_NBRATTESTAB)  END)
*(CASE  WHEN  SUM(A.RRC_ATTCONNESTAB)=0  THEN  0  ELSE  SUM(A.RRC_SUCCCONNESTAB)/SUM(A.RRC_ATTCONNESTAB)  END)  AS  WIRELESS_ACCESS_RATE,
sum(A.ho_succhandoverout) as ho_succhandoverout,
sum(A.ho_succhandoverin) as ho_succhandoverin,
sum(A.sm_succintrammeinterenbx2) as sm_succintrammeinterenbx2,
(sum(A.ho_succhandoverout)+sum(A.ho_succhandoverin)+sum(A.ho_succhandoverin)) as  ho_succ,
sum(A.ho_atthandoverout) as  ho_atthandoverout,
sum(A.ho_atthandoverin)  as ho_atthandoverin,
sum(A.sm_attintrammeinterenbx2)  as sm_attintrammeinterenbx2,
sum(A.ho_atthandoverout)+sum(A.ho_atthandoverin)+sum(A.sm_attintrammeinterenbx2) as ho_att,
case when count(distinct imsi)=0 then 0 else count(case when B.ishigh_price=1 then B.ishigh_price else null end)/count(distinct imsi) end as high_price_imei_rate,
max(h.high_price_imsi_count)  as high_price_imsi_count,
case when count(distinct imsi)=0 then 0 else max(h.high_price_imsi_count)/count(distinct imsi) end as high_price_imsi_rate,
case when (sum(A.ul_data) + sum(A.dl_data))=0 then 0 else (sum(A.ul_data_qci2) + sum(A.dl_data_qci2))/(sum(A.ul_data) + sum(A.dl_data)) end as video_data_rate,
max(ar_do_mo.arpu) as arpu,
max(ar_do_mo.dou) as dou,
max(ar_do_mo.mou) as mou,
max(locknet.locknet_imsi_count) as locknet_imsi_count,
max(turn.turnet_imsi_count) as turnet_imsi_count,
max(offnet.offnet_imsi_count) as offnet_imsi_count
FROM tps_e_g_u_week A 
left join 
(select a.grid_id,sum((c.m_2g*a.xdr_count/4)/b.c1) as g2_flow,sum((c.m_3g*a.xdr_count/4)/b.c1) as g3_flow,sum((c.m_4g*a.xdr_count/4)/b.c1) as g4_flow
from tps_e_g_u_week a,
(select imsi,sum(xdr_count) c1 from tps_e_g_u_week where scan_start_time between '${start_day_str}' and '${stop_day_str}' group by imsi) b,
boco.tb_kr_b_area_user_list_m c
where a.imsi=b.imsi and b.imsi=c.imsi and (scan_start_time between '${start_day_str}' and '${stop_day_str}')
group by a.grid_id ) g2_g3_g4 
on g2_g3_g4.grid_id=A.grid_id 
left join 
(select a.grid_id,sum((c.m_4g*a.xdr_count/4)/b.c1) as g4_flow_volte
from tps_e_g_u_week a,
(select imsi,sum(xdr_count) c1 from tps_e_g_u_week where scan_start_time between '${start_day_str}' and '${stop_day_str}' group by imsi) b,
boco.tb_kr_b_area_user_list_m c
where a.imsi=b.imsi and b.imsi=c.imsi and (scan_start_time between '${start_day_str}' and '${stop_day_str}') and a.volte_user_flag=1
group by a.grid_id ) g4_volte
on g4_volte.grid_id=A.grid_id
left join
(select t.grid_id,count(distinct t.imsi) as high_price_imsi_count from tps_e_g_u_week t,boco.tb_kr_b_area_user_list_m c 
where t.imsi=c.imsi and  (c.avg3m_arpu>200 or c.lm_arpu>200) and (scan_start_time between '${start_day_str}' and '${stop_day_str}')  group by grid_id) h
on h.grid_id=A.grid_id
left join 
(select a.grid_id,sum((c.avg3m_arpu*a.xdr_count/4)/b.c1) as arpu,sum((c.avg3m_dou*a.xdr_count/4)/b.c1) as dou,sum((c.avg3m_mou*a.xdr_count/4)/b.c1) as mou
from tps_e_g_u_week a,
(select imsi,sum(xdr_count) c1 from tps_e_g_u_week where scan_start_time between '${start_day_str}' and '${stop_day_str}' group by imsi) b,
boco.tb_kr_b_area_user_list_m c
where a.imsi=b.imsi and b.imsi=c.imsi and (scan_start_time between '${start_day_str}' and '${stop_day_str}') 
group by a.grid_id ) ar_do_mo
on ar_do_mo.grid_id=A.grid_id
left join
(select t.grid_id,count(distinct t.imsi) as locknet_imsi_count from tps_e_g_u_week t,boco.tb_kr_b_area_user_list_m c 
where t.imsi=c.imsi and  (c.if_sup4g='是' and c.if_usim='是' and (c.user_type<>'4G' or (c.lm_2g<>0 and c.lm_3g<>0 and c.lm_4g is not null and c.lm_4g<>0))) and (scan_start_time between '${start_day_str}' and '${stop_day_str}') 
group by grid_id) locknet
on locknet.grid_id= A.grid_id
left join
(select t.grid_id,count(distinct t.imsi) as turnet_imsi_count from tps_e_g_u_week t,boco.tb_kr_b_area_user_list_m c 
where t.imsi=c.imsi and (c.lm_2g<>0 and c.lm_3g<>0 and (c.lm_4g is null or c.lm_4g=0)) and (c.m_4g is not null and c.m_4g<>0) and (scan_start_time between '${start_day_str}' and '${stop_day_str}') 
group by grid_id ) turn
on turn.grid_id=A.grid_id
left join
(select t.grid_id,count(distinct t.imsi) as offnet_imsi_count from tps_e_g_u_week t,boco.tb_kr_b_area_user_list_m c 
where t.imsi=c.imsi and (c.lm_2g<>0 and c.lm_3g<>0 and c.lm_4g<>0 ) and (c.m_2g is null or c.m_2g=0) and (c.m_3g is null or c.m_2g=0) and (c.m_4g is null or c.m_4g=0)
and (scan_start_time between '${start_day_str}' and '${stop_day_str}') 
group by grid_id) offnet 
on offnet.grid_id=A.grid_id
left join tps_smartphone B 
on substr(A.imei,0,8)=B.imei_tac
left join boco.dim_eutrancell dim
on A.eutrancell_int_id=dim.ec_id
where A.scan_start_time between '${start_day_str}' and '${stop_day_str}' GROUP BY A.region_id,A.grid_id;