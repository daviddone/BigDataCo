package com.boco.gridue;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GridUeReducer extends Reducer<Text, Text, NullWritable, Text> {
    //多目录输出
    private MultipleOutputs<NullWritable, Text> mos;
    private String dest_path = null;
    int i = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	dest_path = context.getConfiguration().get("dest_path");
        mos = new MultipleOutputs(context);
        
    }


    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
    	i = 0;
    	for(Text val:value){
    		System.out.println("val"+val);
        	mos.write(NullWritable.get(),val,key.toString());
        	i++;
        	System.out.println("i:"+i);
        }
    }


    

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

}
