##### 解决内部表空数据 默认为\N
```

	alter table tps_volte_base_extend set serdeproperties ('serialization.null.format' = ''); 

```

##### 添加表字段
```

	alter table tpl_mro_NumOfAdj_eutr_day add columns  (freq_type    int );

```