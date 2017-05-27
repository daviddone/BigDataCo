package com.david.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;


public class HbaseSplitKeyUtils {

    static Logger logger = LoggerFactory.getLogger(HbaseSplitKeyUtils.class);

    //随机生成的rowkey数目
    private int baseRecord;

    //rowkey生成器，大量生成此key，然后划区
    private HashRowKeyGenerator rkGen;

    //取样时，由rowkey数目及region数相除所得的数量，即每个区要放的key数量，以此来界定范围.
    private int splitKeysBase;

    //splitkeys个数，即分区数
    private int splitKeysNumber;

    //由抽样计算出来的splitkeys结果
    private byte[][] splitKeys;

    public HbaseSplitKeyUtils(int baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;

        //实例化rowkey生成器
        rkGen = new HashRowKeyGenerator();

        splitKeysNumber = prepareRegions - 1;
        splitKeysBase = baseRecord / prepareRegions;
    }


    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        //使用treeset保存抽样数据，已排序过
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        int pointer = 0;
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index++;
                }
            }
            pointer++;
        }
        rows.clear();
        rows = null;
        return splitKeys;
    }

    //row生成器
    public class HashRowKeyGenerator {
        private long currentId = 1;
        private long currentTime = System.currentTimeMillis();
        private Random random = new Random();

        public byte[] nextId() {
            try {
                String keyboom= random.nextInt(100000)+"";
                logger.info(currentId+"---:"+keyboom);
                return keyboom.getBytes();
            } finally {
                currentId++;
            }
        }
    }



    public static void main(String[] s){
        //分区数20，测试数据1000000
        HbaseSplitKeyUtils worker = new HbaseSplitKeyUtils(1000,20);
        byte [][] splitKeys = worker.calcSplitKeys();
        //第二个region
        int i=2;
        for(byte[] b:splitKeys){
            logger.info(String.format("第%d个region起始rowkey---:%s\n",i,new String(b)));
            i++;
        }
    }
}

