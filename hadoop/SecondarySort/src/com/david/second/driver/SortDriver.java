package com.david.second.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.david.second.entity.CombinationKeyWritable;
import com.david.second.entity.FirstGroupingComparator;
import com.david.second.entity.FirstPartitioner;
import com.david.second.util.LogUtil;


public class SortDriver extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			ToolRunner.run(new SortDriver(),args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = LogUtil.getConfig();

		Job job = Job.getInstance(conf);
		job.setJarByClass(SortDriver.class);
		job.setJobName(getClass().getName());

		//输入路径
		FileSystem fs = FileSystem.get(conf);
		String mroDir = "/user/boco/second/input/";
		FileInputFormat.addInputPath(job, new Path(mroDir));

		//输出路径
		String mmeTmpDir = "/user/boco/second/output/";
		Path outpath =new Path(mmeTmpDir);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, new Path(mmeTmpDir));

		//map-reduce class
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		//定义分组规则
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        //设置分发到reduce的规则
        job.setPartitionerClass(FirstPartitioner.class);
        
		//设置reduce任务数量
		job.setNumReduceTasks(3);

		//输入、输出文件类型
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//map输出类型
		job.setMapOutputKeyClass(CombinationKeyWritable.class);
		job.setMapOutputValueClass(Text.class);

		//reduce输出类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//将reduce输出文件压缩  
		FileOutputFormat.setCompressOutput(job, false);  //job使用压缩
//		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

}


