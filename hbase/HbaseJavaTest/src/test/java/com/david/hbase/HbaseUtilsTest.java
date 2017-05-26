package com.david.hbase;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;

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
		System.out.println("create done");
	}

	@Test
	public void testCreatTableForce() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteTable() {
		fail("Not yet implemented");
	}

	@Test
	public void testListTables() {
		fail("Not yet implemented");
	}

	@Test
	public void testInsertRowStringStringConfigurationStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testInsertRowStringConfigurationStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleRowStringStringConfigurationStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleRowStringStringConfigurationStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleRowStringStringConfigurationString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetDataStringStringConfigurationStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetDataStringStringConfigurationStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetDataStringStringConfigurationString() {
		fail("Not yet implemented");
	}

	@Test
	public void testScanDataStringConfigurationStringStringInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testScanDataStringConfigurationInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testPrintTables() {
		fail("Not yet implemented");
	}

	@Test
	public void testShowCell() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetFirstCellValue() {
		fail("Not yet implemented");
	}

	@Test
	public void testMain() {
		fail("Not yet implemented");
	}

}
