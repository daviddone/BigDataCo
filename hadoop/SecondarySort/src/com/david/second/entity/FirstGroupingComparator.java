package com.david.second.entity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 定制分组比较器：对规约器值排序
 * @author tangdongqing
 * @Description:  
 * @date 2017年12月26日
 */
public class FirstGroupingComparator extends WritableComparator {


    protected FirstGroupingComparator() {
        super(CombinationKeyWritable.class, true);
    }

    /**
     * 这个比较器控制哪些键要
     * 分组到一个reduce()方法调用
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinationKeyWritable pair1 = (CombinationKeyWritable) a;
        CombinationKeyWritable pair2 = (CombinationKeyWritable) b;
        return pair1.getFirstKey().compareTo(pair2.getFirstKey());
    }
}