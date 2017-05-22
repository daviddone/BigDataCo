package com.david;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.david.finger.util.FingerPrintHdfsUtil;

public class FingerPrintReducer extends Reducer<NullWritable,Text,Text,NullWritable> {
	  
  	private Text result=new Text();
  	private List<String> lists = null;
  	private Map<String,List<String>> map = new HashMap<String,List<String>>();
  	private MultipleOutputs<Text, NullWritable> multipleOutputs;
  	private String destPath = "";
  	@Override
  	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
  			throws IOException, InterruptedException {
  		multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
  		destPath = context.getConfiguration().get("dest_path");
  		System.out.println("destPath:"+destPath);
  	}
    public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	for (Text text : values) {
    		String[] datas = text.toString().split(";");
    		if(datas.length<6){
				continue;
			}
    		String scEnbId = datas[6].split(",")[0];
    		
    		if(map.containsKey(scEnbId)){
    			lists = map.get(scEnbId);
    		}else{
    			lists = new ArrayList<String>();
    		}
    		lists.add(text.toString());
    		map.put(scEnbId, lists);
    		
		}
    	
    }
    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
    		throws IOException, InterruptedException {
    	super.cleanup(context);
    	System.out.println("list.size :"+lists.size());
    	for (Map.Entry<String, List<String>> entry : map.entrySet()) {  
            System.out.println("key = " + entry.getKey() + " and value = " + entry.getValue());  
            FingerPrintHdfsUtil.writeFinger(lists, destPath,result,multipleOutputs,entry.getKey());
        }  
    	multipleOutputs.close();
    }
}
