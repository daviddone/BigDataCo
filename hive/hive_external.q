drop table IF EXISTS tps_pp_g;
create external table IF NOT EXISTS tps_pp_g
(
region_id    bigint,
region_name    String,
city_id    bigint,
city_name    String,
pp_id    bigint,
pp_name    String,
pp_type    bigint,
grid_id    String,
grid_type    bigint,
latitudeld    float,
longitudeld    float,
latituderu    float,
longituderu    float,
latituderd    float,
longituderd    float,
latitudelu    float,
grid_center_geohash    String,
longitudelu    float,
insert_time    String
)  partitioned by (datepart string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/boco/cdr/input/ppg/20170830/';
alter table tps_pp_g add partition(datepart='20170830') location '/user/boco/cdr/input/ppg/20170830/';

#数据路径 /user/boco/cdr/input/ppg/20170830
#数据样例 -257096336,哈尔滨市,-1,\ ,-25709633627232,0451-27232-哈尔滨,\ ,52TDR-9165-6610-5,\ ,45.448874067260853,128.30720246844561,45.448919457828701,128.3072658535481,45.448874455040702,128.30726640444684,45.448919070048241,\ ,128.30720191749603,2017-08-08 00:00:00
#数据核查  select * from tps_pp_g limit 3;