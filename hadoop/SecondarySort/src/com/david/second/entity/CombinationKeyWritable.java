package com.david.second.entity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义组合键
 * @author tangdongqing
 * @Description: 
 * @date 2017年12月26日
 */
public class CombinationKeyWritable implements WritableComparable<CombinationKeyWritable> {
	private Text firstKey;
    private IntWritable secondKey;
    
    public CombinationKeyWritable() {
        this.firstKey = new Text();
        this.secondKey = new IntWritable();
    }
    
    public Text getFirstKey() {
        return this.firstKey;
    }
    
    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }
    
    public IntWritable getSecondKey() {
        return this.secondKey;
    }
    
    public void setSecondKey(IntWritable secondKey) {
        this.secondKey = secondKey;
    }
    
    @Override
    public void readFields(DataInput dateInput) throws IOException {
        this.firstKey.readFields(dateInput);
        this.secondKey.readFields(dateInput);
    }
    
    @Override
    public void write(DataOutput outPut) throws IOException {
        this.firstKey.write(outPut);
        this.secondKey.write(outPut);
    }
    
    /**
    * 自定义比较策略
    * 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
    * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整) 二次排序
    */
    @Override
    public int compareTo(CombinationKeyWritable combinationKey) {
    	int cmp = this.firstKey.compareTo(combinationKey.getFirstKey());
        if (cmp != 0) {  
            return cmp;  
          }
        return -this.secondKey.compareTo(combinationKey.secondKey);  //默认升序,加上负号-为降序
    }
}
 