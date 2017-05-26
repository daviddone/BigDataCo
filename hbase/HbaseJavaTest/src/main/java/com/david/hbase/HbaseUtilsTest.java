package com.david.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseUtilsTest {
	static Configuration config = HBaseConfiguration.create();
	static{
//		推荐 部署方式
//	    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

		config.set("hbase.zookeeper.quorum", "vm10.60.0.11.com.cn,vm10.60.0.7.com.cn,vm10.60.0.8.com.cn");// hbase-site.xml中zookeeper的配置
	    config.set("hbase.zookeeper.property.clientPort", "2181");
	    config.setInt("hbase.rpc.timeout",20000);
	    config.setInt("hbase.client.operation.timeout",30000);
	    config.setInt("hbase.client.scanner.timeout.period",200000);
	}
	
	public static void main(String[] args) throws Exception {
		//建表
		String tableName = "passage";
		String[] family = {"author","info"}; 
		byte[][] splitbytes = null;
		HbaseUtils.creatTable(tableName, family, config, splitbytes);
	}
	
}
