package com.david.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.msg.cals_en_US;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtils {
	static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);

	/*
	 * 不强制性创建表
	 * 
	 * @tableName 表名
	 * 
	 * @family 列族列表
	 * 
	 * @config　配置信息
	 * 
	 * @splitbytes region分区
	 * 
	 * 测试用,请手动加命名空间
	 */
	public static void creatTable(String tableName, String[] family,
			Configuration config, byte[][] splitbytes) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor desc = new HTableDescriptor(
					TableName.valueOf(tableName));
			for (int i = 0; i < family.length; i++) {
				desc.addFamily(new HColumnDescriptor(family[i]));
			}
			if (admin.tableExists(desc.getTableName())) {
				throw new Exception("table Exists!");
			} else {
				if (splitbytes == null) {
					admin.createTable(desc);
				} else {
					admin.createTable(desc, splitbytes);
				}
			}
		}
	}

	/*
	 * 强制性创建表
	 * 
	 * @tableName 表名
	 * 
	 * @family 列族列表
	 * 
	 * @config　配置信息
	 * 
	 * 测试用，请手动加命名空间
	 */
	public static void creatTableForce(String tableName, String[] family,
			Configuration config, byte[][] splitbytes) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor desc = new HTableDescriptor(
					TableName.valueOf(tableName));
			for (int i = 0; i < family.length; i++) {
				desc.addFamily(new HColumnDescriptor(family[i]));
				desc.setRegionReplication(6);
			}
			TableName tn = desc.getTableName();
			if (admin.tableExists(tn)) {
				try {
					admin.disableTable(tn);
				} catch (TableNotEnabledException e) {
					logger.error("表已禁用");
				}
				admin.deleteTable(tn);
			}
			if (splitbytes == null) {
				admin.createTable(desc);
			} else {
				admin.createTable(desc, splitbytes);
			}
		}
	}

	/*
	 * 删表
	 * 
	 * @tableName 表名
	 * 
	 * @config 配置信息
	 * 
	 * 测试用
	 */
	public static void deleteTable(String tableName, Configuration config)
			throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			TableName tn = TableName.valueOf(tableName);
			if (admin.tableExists(tn)) {
				try {
					admin.disableTable(tn);
				} catch (TableNotEnabledException e) {
					logger.error("表已禁用");
				}
				admin.deleteTable(tn);
			}
		}
	}

	/*
	 * 查看已有表
	 * 
	 * ＠config　配置信息
	 * 
	 * 测试用
	 */
	public static HTableDescriptor[] listTables(Configuration config)
			throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor hTableDescriptors[] = admin.listTables();
			return hTableDescriptors;
		}
	}

	/*
	 * 插入数据
	 * 
	 * @namespace 命名空间
	 * 
	 * @tableName　表名
	 * 
	 * @config 配置信息
	 * 
	 * @rowkey 行key
	 * 
	 * @colFamily 列族
	 * 
	 * @col　子列
	 * 
	 * @val　值
	 */
	public static void insertRow(String namespace, String tableName,
			Configuration config, String rowkey, String colFamily, String col,
			String val) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			if (namespace != null) {
				tableName = namespace + ":" + tableName;
			}
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowkey));
			put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col),
					Bytes.toBytes(val));
			table.put(put);
			table.close();
		}
	}

	public static void insertRow(String tableName, Configuration config,
			String rowkey, String colFamily, String col, String val)
			throws Exception {
		insertRow(null, tableName, config, rowkey, colFamily, col, val);
	}

	/*
	 * 删除数据
	 * 
	 * @namespace 命名空间
	 * 
	 * @tableName　表名
	 * 
	 * @config 配置信息
	 * 
	 * @rowkey 行key
	 * 
	 * @colFamily 列族
	 * 
	 * @col　子列
	 */
	public static void deleRow(String namespace, String tableName,
			Configuration config, String rowkey, String colFamily, String col)
			throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			if (namespace != null) {
				tableName = namespace + ":" + tableName;
			}
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			// 删除指定列族
			if (colFamily != null && col == null)
				delete.addFamily(Bytes.toBytes(colFamily));
			// 删除指定列
			if (colFamily != null && col != null)
				delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			table.delete(delete);
			table.close();
		}
	}

	public static void deleRow(String namespace, String tableName,
			Configuration config, String rowkey, String colFamily)
			throws Exception {
		deleRow(namespace, tableName, config, rowkey, colFamily, null);
	}

	public static void deleRow(String namespace, String tableName,
			Configuration config, String rowkey) throws Exception {
		deleRow(namespace, tableName, config, rowkey, null, null);
	}

	/*
	 * 根据rowkey查找数据
	 * 
	 * @namespace 命名空间
	 * 
	 * @tableName　表名
	 * 
	 * @config 配置信息
	 * 
	 * @rowkey 行key
	 * 
	 * @colFamily 列族
	 * 
	 * @col　子列
	 */
	public static Result getData(String namespace, String tableName,
			Configuration config, String rowkey, String colFamily, String col)
			throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			if (namespace != null) {
				tableName = namespace + ":" + tableName;
			}
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			if (colFamily != null && col == null)
				get.addFamily(Bytes.toBytes(colFamily));
			if (colFamily != null && col != null)
				get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			Result result = table.get(get);
			table.close();
			return result;
		}
	}

	public static Result getData(String namespace, String tableName,
			Configuration config, String rowkey, String colFamily)
			throws Exception {
		return getData(namespace, tableName, config, rowkey, colFamily, null);
	}

	public static Result getData(String namespace, String tableName,
			Configuration config, String rowkey) throws Exception {
		return getData(namespace, tableName, config, rowkey, null, null);
	}

	/*
	 * 批量查找数据
	 * 
	 * @table 表名
	 * 
	 * @config配置文件
	 * 
	 * @startRow 开始的行key
	 * 
	 * @stopRow　停止的行key
	 * 
	 * hbase会将自己的元素按照key的ASCII码排序 找出5193开头的元素
	 * 
	 * 5193:1 5193:2 5194:1 51939:1 51942:1
	 * 
	 * scan.setStartRow("5193:#"); scan.setStopRow("5193::");
	 * 
	 * 原因：ASCII排序中："#" < "0-9" < ":" 取出来的将是5193:后面跟着数字的元素
	 * 
	 * 测试用
	 */
	public static List<Result> scanData(String tableName, Configuration config,
			String startRow, String stopRow, int limit) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if (startRow != null && stopRow != null) {
				scan.setStartRow(Bytes.toBytes(startRow));
				scan.setStopRow(Bytes.toBytes(stopRow));
			}
			scan.setBatch(limit);
			List<Result> result = new ArrayList<Result>();
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				result.add(r);
			}
			table.close();
			return result;
		}
	}

	public static List<Result> scanData(String tableName, Configuration config,
			int limit) throws Exception {
		return scanData(tableName, config, null, null, limit);
	}

	// rowkey 行的模糊查询 RowFilter
	public static List<Result> filterRowkeyData(String tableName,
			Configuration config, 
			String filterInfo) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(filterInfo));
			scan.setFilter(filter);
			List<Result> result = new ArrayList<Result>();
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				result.add(r);
			}
			System.out.println("result.size"+result.size());
			table.close();
			return result;
		}
	}
	// column 按列模糊查询
	public static List<Result> filterColumnData(String tableName,
			Configuration config, String colFamily, String col,
			String filterInfo) throws Exception {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes(colFamily),
					Bytes.toBytes(col),
					CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(filterInfo)
					);
			scan.setFilter(filter);
			List<Result> result = new ArrayList<Result>();
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				result.add(r);
			}
			System.out.println("result.size"+result.size());
			table.close();
			return result;
		}
	}

	/*
	 * 打印表
	 * 
	 * @tables 打印的表描述对象
	 */
	public static void printTables(HTableDescriptor[] tables) {
		for (HTableDescriptor t : tables) {
			HColumnDescriptor[] columns = t.getColumnFamilies();
			logger.info(String.format("tables:%s,columns-family:\n",
					t.getTableName()));
			for (HColumnDescriptor column : columns) {
				// logger.info(String.format("\t%s\n",
				// column.getNameAsString()));
				logger.info(column.getNameAsString());
			}
		}
	}

	/*
	 * 格式化输出
	 * 
	 * @result 结果
	 */
	public static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			logger.info("RowName:" + new String(CellUtil.cloneRow(cell)) + "");
			logger.info("Timetamp:" + cell.getTimestamp() + "");
			logger.info("column Family:"
					+ new String(CellUtil.cloneFamily(cell)) + "");
			logger.info("row Name:" + new String(CellUtil.cloneQualifier(cell))
					+ "");
			logger.info("value:" + new String(CellUtil.cloneValue(cell)) + "");
			logger.info("---------------");
		}
	}

	public static String getFirstCellValue(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			return new String(CellUtil.cloneValue(cell)) + "";
		}
		return null;
	}

	public static void main(String... args) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				"vm10.60.0.11.com.cn,vm10.60.0.7.com.cn,vm10.60.0.8.com.cn");// hbase-site.xml中zookeeper的配置
		config.set("hbase.zookeeper.property.clientPort", "2181");
		String namespace = "namespace";
		String tablename = "passage";
		String[] column_family = { "info" };
		try {

			// 列出表信息
			HTableDescriptor[] tables = listTables(config);
			printTables(tables);
			// 为同一条数据 按列赋值
			insertRow(tablename, config, 2 + "", column_family[0], "title",
					"文章标题222");
			insertRow(tablename, config, 2 + "", column_family[0], "content",
					"zhangsan");
			// 获取单行值
			Result result = getData(null, tablename, config, "2",
					column_family[0]);
			showCell(result);

			// 扫描表，获取前20行
			List<Result> results = scanData(tablename, config, 20);
			for (Result r : results) {
				showCell(r);
			}
			deleRow(null, tablename, config, "2", column_family[0]);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
