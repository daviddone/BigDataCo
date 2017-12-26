package com.david.second.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.david.second.entity.CombinationKeyWritable;

public class SortReducer extends Reducer<CombinationKeyWritable, Text, NullWritable, Text> {
	
	private List<String> lists = null;
	private Text outText  = new Text();
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void setup(Context context) throws IOException, InterruptedException {
		lists =  new ArrayList<>();
	}
	
	@Override
	protected void reduce(CombinationKeyWritable  key, Iterable<Text> value, Context context) 
			throws IOException, InterruptedException {
		lists.clear();
		for (Iterator<Text> it = value.iterator(); it.hasNext();) {
			Text text = it.next();
			lists.add(text.toString());
			System.out.println(text.toString());
			if(lists.size()==2){//获取前2条数据
				break;
			}
		}
		System.out.println("lists:"+lists.size());
		for(String item:lists){
			outText.set(item);
			context.write(NullWritable.get(),outText );  //核查顺序
		}
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	
	}
}

