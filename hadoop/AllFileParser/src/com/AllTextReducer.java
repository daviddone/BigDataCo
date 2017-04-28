package com;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AllTextReducer extends Reducer<Text,Text,Text,Text> {
	  
  	private Text result=new Text();
  
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	
    	result.set("Test");
    	System.out.println(key.toString()+result.toString());
    	context.write(key, result); //Reducer<Text,Text,Text,Text> 后两位一致
    }
}
