package com.boco.gridue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import signal.util.LogUtil;

public class GridUeDriver extends Configured implements Tool{


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("参数有误，确保输入，输出路径");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		String step1Path = args[1];
		Configuration config = LogUtil.getConfig();
		 config.set("dest_path",step1Path);
		//设置map的分片大小.设置为128M
//		 config.set("dfs.block.size", "134217728");//128M
//		 config.setLong(CombineFileInputFormat.SPLIT_MAXSIZE, 128*1024*1024);
//		 config.set("dfs.block.size", "67108864");//64M
//		 config.setLong(CombineFileInputFormat.SPLIT_MAXSIZE, 64*1024*1024);
		 
		 //设置超时时间
		 config.set("mapreduce.task.timeout", "420000");
		 config.set("dfs.socket.timeout", "420000");
		 config.set("dfs.datanode.socket.write.timeout", "420000");
		 
//		 config.set("mapreduce.map.memory.mb", "2048");//2G
//		 config.set("mapreduce.map.java.opts", "-Xmx2048m");//2G
//		 config.set("mapreduce.reduce.memory.mb", "4096");//4G
//		 config.set("mapreduce.reduce.java.opts", "-Xmx4000m");//4G
		 
		 Job job = Job.getInstance(config);
		 job.setJarByClass(GridUeDriver.class);
		 String jobName = getClass().getName();
		 job.setJobName(jobName);
		 
	     
	     //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
	     
	     //part-r-00000 空文件不生成  
	     LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	     job.setMapperClass(GridUeMapper.class);
	     job.setReducerClass(GridUeReducer.class);
	     FileOutputFormat.setCompressOutput(job, false); 
	     
//	     job.setNumReduceTasks(0);
//	     job.setNumReduceTasks(200);
	     
	     FileSystem fs =FileSystem.get(config);
	     Path outpath =new Path(step1Path);
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
		
		FileInputFormat.addInputPaths(job, args[0]);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	
	public static void main(String[] args) throws Exception{
//		String[] args = {"/user/boco/david/finger/input/grid_ue20_imsi.csv","/user/boco/david/finger/output81/"};
		System.out.println(args.toString());
		int ret = ToolRunner.run(new GridUeDriver(), args);
		System.out.println("gridue"+ret);
		if(ret == 1){
			System.out.println("gridue step failed.....");
			System.exit(ret);
		}
//		System.exit(ret);
	}
	
}
