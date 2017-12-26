##### hadoop源码级修改
用途：mos输出多文件时，会指定加上r-000n的文件名，采用此方法去掉r-000n   <br>
使用方式：LazyOutputFormat.setOutputFormatClass(job, com.david.mos.out.TextOutputFormat.class);