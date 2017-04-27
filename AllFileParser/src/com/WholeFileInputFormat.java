package com;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//make a wholeFileInputFormat
//referenced from URL(http://stackoverflow.com/questions/30649590/using-wholefileinputformat-with-hadoop-mapreduce-still-results-in-mapper-process)
public class WholeFileInputFormat extends FileInputFormat <NullWritable, BytesWritable>{

	@Override
	protected boolean isSplitable(JobContext context, Path file){
			return false;
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}

}