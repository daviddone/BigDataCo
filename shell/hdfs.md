#### hdfs常用命令

##### 列出文件
hdfs dfs -ls /user/david

##### 删除文件
hdfs dfs -rm -r oozie_wy/lib/*  删除文件夹下所有文件 <br>
hdfs dfs -rm -r oozie_wy/schedule 删除指定文件夹

##### 文件移动到另外的路径下：
hdfs dfs -mkdir gx_mro/input/   <br>
hdfs dfs -mv gx_mro/lte_mro_ZTE_2017070423_20170718073025362_816 gx_mro/input/<br>

##### 查看hdfs上文件大小
hdfs dfs -du -h /user/david/  列出文件夹下的所有文件夹大小<br>
hdfs dfs -du -s -h /user/david/  列出指定文件夹大小

##### 查看复制因子
hdfs fsck /user/boco/cdr/output/gridue

##### 查看任务并杀掉
hadoop job -list  <br>
hadoop job -kill job_876554

##### oozie杀掉job 
oozie job -oozie http://localhost:11000/oozie -kill jobID  <br>
oozie job -oozie http://localhost:11000/oozie -kill 0000004-170914151823015-oozie-oozi-W

##### oozie查看任务的外部id
oozie job -oozie http://localhost:11000/oozie -info 0000004-170914151823015-oozie-oozi-W