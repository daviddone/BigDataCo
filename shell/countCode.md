##### 将文件夹下的java类内容合并
```

find -name "*.java" -exec 'cat' {} \; > allText

```

##### 统计文件行数
wc -l allText


##### 统计文件夹下包含某个字符串的所有文件
```

find .|xargs grep -ri "join" -l

```