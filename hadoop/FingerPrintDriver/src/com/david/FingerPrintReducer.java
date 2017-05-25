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
import com.david.finger.util.JVMUtil;

public class FingerPrintReducer extends Reducer<NullWritable,Text,Text,NullWritable> {
	  
  	private Text result=new Text();
  	private List<String> lists = new ArrayList<String>();
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
//    	super.cleanup(context);
    	for (Map.Entry<String, List<String>> entry : map.entrySet()) {  
            System.out.println("key = " + entry.getKey());
            System.out.println(JVMUtil.getMemoryStatus());
            lists = entry.getValue();
            try {
            	FingerPrintHdfsUtil.writeFinger(lists, destPath,result,multipleOutputs,entry.getKey());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("此条数据 有误  entry.getKey()");
			}
        }  
    	multipleOutputs.close();
    }
}
