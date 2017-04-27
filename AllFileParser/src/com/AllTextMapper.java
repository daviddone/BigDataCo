package com;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class AllTextMapper extends Mapper<NullWritable, BytesWritable, Text, Text>{

//  	private final static IntWritable one = new IntWritable(1);
  	private Text mapkey = new Text();
  	private Text valueTuple = new Text();
	private  String previous="PreviousGram",current="currentGram";
  	
  	public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {	
  		  String valueString = new String(value.copyBytes(),"UTF-8");
  		  mapkey.set(valueString);
		  valueTuple.set(valueString); 
		  context.write(mapkey, valueTuple);
  	}

}
