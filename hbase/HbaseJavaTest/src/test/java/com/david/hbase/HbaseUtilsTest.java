package com.david.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseUtilsTest {
	static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
	
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

	@Before
	public void setUp() throws Exception {
		//init config here
	}

	@Test
	public void testCreatTable() throws Exception {
		//建表
		String tableName = "passage";
		String[] family = {"author","info"}; 
		byte[][] splitbytes = null; 	
		HbaseUtils.creatTable(tableName, family, config, splitbytes);
		logger.info("create table done ");
	}

	@Test
	public void testDeleteTable() {
		fail("Not yet implemented");
	}

	@Test
	public void testListTables() throws IOException {
		//列出表信息
        HTableDescriptor[] tables = HbaseUtils.listTables(config);
        HbaseUtils.printTables(tables);
	}

	

}
