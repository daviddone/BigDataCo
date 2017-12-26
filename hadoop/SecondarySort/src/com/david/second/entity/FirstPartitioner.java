package com.david.second.entity;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 * @author tangdongqing
 * @Description: 
 * @date 2017年12月26日
 * @param <V>
 */
public class FirstPartitioner<V> extends Partitioner<CombinationKeyWritable, V> {

    /**
     * 对组合键按第一个自然键排序分组
     *如果不自定义分区的话，mapreduce框架会根据默认的hash分区方法，
     * 将整个组合将相等的分到一个分区中，这样的话显然不是我们要的效果
     * @param myPair
     * @param v
     * @param numPartitions
     * @return
     */
    @Override public int getPartition(CombinationKeyWritable key, V v, int numPartitions) {

    	return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}