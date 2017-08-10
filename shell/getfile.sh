hdfs dfs -get /user/boco/cdr/output/gridue/20170721/
cd 20170721
find -name "*-r*" -exec 'cat' {} \; > allText
wc -l allText
