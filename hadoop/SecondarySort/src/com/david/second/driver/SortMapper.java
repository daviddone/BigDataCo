package com.david.second.driver;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.david.second.entity.CombinationKeyWritable;
import com.david.second.util.Tools;

public class SortMapper extends Mapper<LongWritable, Text, CombinationKeyWritable, Text> {
	
	private Text outText = new Text();
	private CombinationKeyWritable combineKey = new CombinationKeyWritable();
	private String dataKey = "";
	private String dataValue = "";
	
	
	protected void setup(Context context) throws IOException, InterruptedException {
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[]  lineColumns = Tools.splitString(value.toString(), "|");
		if(lineColumns.length<2){
			return;
		}
		dataKey = lineColumns[0];
		dataValue = lineColumns[1];
		combineKey.setFirstKey(new Text(dataKey));
		combineKey.setSecondKey(new IntWritable(Integer.parseInt(dataValue)));
		
		outText.set(value.toString());
		context.write(combineKey, outText);
	}
}


