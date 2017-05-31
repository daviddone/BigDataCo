package com.david.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
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
	public void testInsertRow() throws Exception {
		String namespace="namespace";
		String tablename = "passage";
		String[] column_family = {"info"};
        //插入10000行
		for (int i = 1; i < 20; i++){
			HbaseUtils.insertRow(tablename, config, i+"", column_family[0], "title", "好消息标题"+i);
		}
	}
	
	@Test
	public void testDeleteTable() throws Exception {
		String tablename = "passage";
		HbaseUtils.deleteTable(tablename, config);
	}

	@Test
	public void testListTables() throws IOException {
		//列出表信息
        HTableDescriptor[] tables = HbaseUtils.listTables(config);
        HbaseUtils.printTables(tables);
	}
	
	@Test
	public void testScanData() throws Exception {
		String tablename = "passage";
		List<Result> results = HbaseUtils.scanData(tablename, config, 1);
		for (Result r : results) {
			HbaseUtils.showCell(r);
        }
	}
	@Test
	public void testScanDataRowKey() throws Exception {
		String tablename = "passage";
		List<Result> results = HbaseUtils.scanData(tablename, config, "1", "26", 10);
		for (Result r : results) {
			HbaseUtils.showCell(r);
		}
	}
	
	@Test
	public void testUpdateDataByRowKey() throws Exception {
		String tablename = "passage";
		HbaseUtils.insertRow(tablename, config, 9+"", "info", "title",
				"zhangsan");
	}
	
	@Test
	public void testFilterRowKeyData() throws Exception {
		String tableName = "passage";
		String filterInfo = "1";
		List<Result> results = HbaseUtils.filterRowkeyData(tableName, config, filterInfo);
		for (Result r : results) {
			HbaseUtils.showCell(r);
		}
	}
	@Test
	public void testFilterColumnData() {
		try {
			String tableName = "passage";
			String colFamily = "info";
			String col = "title";
//			String filterInfo = "zhangsan";
			String filterInfo = "好消息";// 中文模糊查詢 
			List<Result> results = HbaseUtils.filterColumnData(tableName, config, colFamily, col, filterInfo);
			for (Result r : results) {
				HbaseUtils.showCell(r);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
