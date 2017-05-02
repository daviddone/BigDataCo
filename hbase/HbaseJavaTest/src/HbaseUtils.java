

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HbaseUtils {

    private static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
    private static Configuration conf;
    private static HBaseAdmin admin;


    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "vm10.60.0.11.com.cn,vm10.60.0.9.com.cn,vm10.60.0.8.com.cn");//hbase配置的zookeeper
		conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            logger.error("", e);
        }
    }



    /**
     * 创建一个表
     * @param tabName 表名
     * @param cols    列族
     * @return
     */
    public static String createTab(String tabName, String... cols) {

        try {
            //如果存在这个表，则删除它
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                logger.info("删除表成功：{}", tabName);
            }

            HTableDescriptor tab = new HTableDescriptor(TableName.valueOf(tabName));

            for (String col : cols) {
                HColumnDescriptor c = new HColumnDescriptor(col);
                c.setMaxVersions(1);
                tab.addFamily(c);
            }
            admin.createTable(tab);
            logger.info("表创建成功：{}", tab);
            return tabName;
        } catch (Exception e) {
            logger.error("创建表出现异常：" + tabName, e);
        }
        return null;
    }

    /**
     * 删除表
     *
     * @param tabName
     */
    public static void dropTab(String tabName) {

        try {
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                logger.info("已经删除表：{}", tabName);
            } else {
                logger.info("要删除的表不存在：{}", tabName);
            }
        } catch (IOException e) {
            logger.error("删除表出现异常：" + tabName, e);
        }
    }

    /**
     * 添加或者更新一行数据
     *
     * @param tabName 表名
     * @param rowkey  行键
     * @param colfam  列族
     * @param map     插入的键值对
     */
    public static void addRow(String tabName, String rowkey, String colfam, Map<String, String> map) {

        try {
            if (admin.tableExists(tabName)) {
                HTable tab = new HTable(conf, tabName);

                Put p = new Put(Bytes.toBytes(rowkey));//行键
                byte[] colbytes = Bytes.toBytes(colfam);//列族

                Set<String> keySet = map.keySet();
                for (String key : keySet) {
                    String val = map.get(key);
                    p.add(colbytes, Bytes.toBytes(key), Bytes.toBytes(val));
                }
                tab.put(p);
                tab.close();
            } else {
                logger.info("表不存在:{}", tabName);
            }
        } catch (IOException e) {
            logger.error("添加数据出现异常：" + tabName, e);
        }
    }

    /**
     * 查找一行数据
     *
     * @param tabName   表名
     * @param rowkey    行键
     * @param colfamily 列族
     * @param cols      所有列的数组
     * @return
     */
    public static Map<String, String> getRow(String tabName, String rowkey, String colfamily, String... cols) {

        Map<String, String> map = new HashMap<String, String>();
        try {
            if (!admin.tableExists(tabName)) {
                logger.info("表不存在：{}", tabName);
                return null;
            }

            HTable tab = new HTable(conf, tabName);
            Get g = new Get(Bytes.toBytes(rowkey));
            g.addFamily(Bytes.toBytes(colfamily));
            Result r = tab.get(g);
            for (String col : cols) {
                byte[] valbyte = r.getValue(Bytes.toBytes(colfamily), Bytes.toBytes(col));
                String val = Bytes.toString(valbyte);
                map.put(col, val);
            }
        } catch (IOException e) {
            logger.error("", e);
        }
        return map;
    }


    public static void delRow(String tabName, String rowkey) {

        try {
            if (!admin.tableExists(tabName)) {
                logger.info("表不存在：{}", tabName);
                return;
            }

            HTable tab = new HTable(conf, tabName);
            Delete del = new Delete(Bytes.toBytes(rowkey));
            tab.delete(del);
            logger.info("删除成功：{}-->{}", tabName, rowkey);
        } catch (IOException e) {
            logger.error("", e);
        }
    }


}
