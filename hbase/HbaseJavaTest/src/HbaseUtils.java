

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
        conf.set("hbase.zookeeper.quorum", "vm10.60.0.11.com.cn,vm10.60.0.9.com.cn,vm10.60.0.8.com.cn");//hbase���õ�zookeeper
		conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            logger.error("", e);
        }
    }



    /**
     * ����һ����
     * @param tabName ����
     * @param cols    ����
     * @return
     */
    public static String createTab(String tabName, String... cols) {

        try {
            //��������������ɾ����
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                logger.info("ɾ����ɹ���{}", tabName);
            }

            HTableDescriptor tab = new HTableDescriptor(TableName.valueOf(tabName));

            for (String col : cols) {
                HColumnDescriptor c = new HColumnDescriptor(col);
                c.setMaxVersions(1);
                tab.addFamily(c);
            }
            admin.createTable(tab);
            logger.info("�����ɹ���{}", tab);
            return tabName;
        } catch (Exception e) {
            logger.error("����������쳣��" + tabName, e);
        }
        return null;
    }

    /**
     * ɾ����
     *
     * @param tabName
     */
    public static void dropTab(String tabName) {

        try {
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                logger.info("�Ѿ�ɾ����{}", tabName);
            } else {
                logger.info("Ҫɾ���ı����ڣ�{}", tabName);
            }
        } catch (IOException e) {
            logger.error("ɾ��������쳣��" + tabName, e);
        }
    }

    /**
     * ��ӻ��߸���һ������
     *
     * @param tabName ����
     * @param rowkey  �м�
     * @param colfam  ����
     * @param map     ����ļ�ֵ��
     */
    public static void addRow(String tabName, String rowkey, String colfam, Map<String, String> map) {

        try {
            if (admin.tableExists(tabName)) {
                HTable tab = new HTable(conf, tabName);

                Put p = new Put(Bytes.toBytes(rowkey));//�м�
                byte[] colbytes = Bytes.toBytes(colfam);//����

                Set<String> keySet = map.keySet();
                for (String key : keySet) {
                    String val = map.get(key);
                    p.add(colbytes, Bytes.toBytes(key), Bytes.toBytes(val));
                }
                tab.put(p);
                tab.close();
            } else {
                logger.info("������:{}", tabName);
            }
        } catch (IOException e) {
            logger.error("������ݳ����쳣��" + tabName, e);
        }
    }

    /**
     * ����һ������
     *
     * @param tabName   ����
     * @param rowkey    �м�
     * @param colfamily ����
     * @param cols      �����е�����
     * @return
     */
    public static Map<String, String> getRow(String tabName, String rowkey, String colfamily, String... cols) {

        Map<String, String> map = new HashMap<String, String>();
        try {
            if (!admin.tableExists(tabName)) {
                logger.info("�����ڣ�{}", tabName);
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
                logger.info("�����ڣ�{}", tabName);
                return;
            }

            HTable tab = new HTable(conf, tabName);
            Delete del = new Delete(Bytes.toBytes(rowkey));
            tab.delete(del);
            logger.info("ɾ���ɹ���{}-->{}", tabName, rowkey);
        } catch (IOException e) {
            logger.error("", e);
        }
    }


}
