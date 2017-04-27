package com;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//referenced from URL(http://stackoverflow.com/questions/30649590/using-wholefileinputformat-with-hadoop-mapreduce-still-results-in-mapper-process)
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable>{
	private FileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException{
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException{
		if(!processed){
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try{
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			}finally{
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		//do nothing
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress(){
		// TODO Auto-generated method stub
		return processed ? 1.0f : 0.0f;
	}
}     
