##### 解决内部表空数据 默认为\N
```

	alter table tps_volte_base_extend set serdeproperties ('serialization.null.format' = ''); 

```