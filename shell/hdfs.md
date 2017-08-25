#### hdfs常用命令

##### 列出文件
hdfs dfs -ls /user/david


##### 文件移动到另外的路径下：
hdfs dfs -mkdir gx_mro/input/   <br>
hdfs dfs -mv gx_mro/lte_mro_ZTE_2017070423_20170718073025362_816 gx_mro/input/<br>

##### 查看hdfs上文件大小
hdfs dfs -du -h /user/david/  列出文件夹下的所有文件夹大小<br>
hdfs dfs -du -s -h /user/david/  列出指定文件夹大小

