package com.david;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FingerPrintMapper extends Mapper<Object,Text, NullWritable, Text>{

  	private Text valueTuple = new Text();
  	
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
		  valueTuple.set(value.toString()); 
		  context.write(NullWritable.get(), valueTuple);
  	}

}
