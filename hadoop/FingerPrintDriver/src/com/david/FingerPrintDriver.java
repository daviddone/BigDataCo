package com.david;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FingerPrintDriver extends Configured implements Tool{


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("参数有误，确保输入，输出路径参数存在");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Configuration config = new Configuration();
		 //本地测试
//		 config.set("fs.defaultFS", "hdfs://vm10.60.0.11.com.cn:8020/"); 
//		 config.set("yarn.resourcemanager.hostname", "vm10.60.0.11.com.cn");
		 
		 config.set("dest_path",args[1]);
		 Job job = Job.getInstance(config, "Ngram");

		 job.setJarByClass(FingerPrintJob.class);
		 
		 //设置文件输入类型
		 job.setInputFormatClass(CombineTextInputFormat.class);
	     job.setOutputKeyClass(NullWritable.class);
	     job.setOutputValueClass(Text.class);
	     
	     //part-r-00000 空文件不生成
	     LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	     
	     job.setMapperClass(FingerPrintMapper.class);
	     job.setReducerClass(FingerPrintReducer.class);
	     
	     job.setNumReduceTasks(1);
	     
	     FileSystem fs =FileSystem.get(config);
	     Path outpath =new Path(args[1]);
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
		//设置查找子文件
	    FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	
	public static void main(String[] args) throws Exception{
//		String[] args = {"/user/boco/david/finger/input/","/user/boco/david/finger/output/"};
		int ret = ToolRunner.run(new FingerPrintDriver(), args);
		System.exit(ret);
	}
	
}
