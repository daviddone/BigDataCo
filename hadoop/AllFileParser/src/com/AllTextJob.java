package com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllTextJob {
	
	//动态传参 注意修改argslocal
	public static void main(String[] argslocal) throws Exception {
		 String[] args = {"/user/boco/david/finger/input/","/user/boco/david/finger/output/"};
		 Configuration config = new Configuration();
		 //本地测试
		 config.set("fs.defaultFS", "hdfs://boco:8020/"); 
		 config.set("yarn.resourcemanager.hostname", "boco");
		 
		 Job job = Job.getInstance(config, "Ngram");

		 job.setJarByClass(AllTextJob.class);
		 
		 job.setInputFormatClass(WholeFileInputFormat.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(Text.class);
	     
	     job.setMapperClass(AllTextMapper.class);
	     //job.setCombinerClass(NgramReducer.class);
	     job.setReducerClass(AllTextReducer.class);
	     
	     job.setNumReduceTasks(1);
	     
	     FileSystem fs =FileSystem.get(config);
	     Path outpath =new Path(args[1]);
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
	     WholeFileInputFormat.addInputPath(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	     System.exit(job.waitForCompletion(true)?0:1);
	   }
}
