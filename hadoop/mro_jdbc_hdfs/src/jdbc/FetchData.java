package jdbc;

import java.io.File;

import org.apache.hadoop.fs.Path;

public class FetchData {
	private String _dbDataDir = "";
	private String _cfgDir = "";
	private int _type = 0;
	private String _dbType = "";
	private String _splitStr = "";
	
	public FetchData(String dbDataDir, String cfgDir, int type, String dbType, String splitStr) {
		_dbDataDir = dbDataDir;
		_cfgDir = cfgDir;
		_type = type;
		_dbType = dbType;
		_splitStr = splitStr;
	}
	
	public FetchData() {
		// TODO Auto-generated constructor stub
	}

	public void execute() {
		Jdbc db = new Jdbc(_cfgDir, _dbType, _type, _splitStr);
		db.connect();
		
		FileUtils fileUtils = new FileUtils();
		fileUtils.mkdir(new File(_dbDataDir));
		
		String sqlLte = makeSqlLte();
		String result = db.executeSql(sqlLte);
		System.out.println(result);
		FileUtils.writeLine(new Path("cdr/input/ppg/ppg.csv"), result);//hdfs上写入文件 cdr/input/ppg
	}
	
	private String makeSqlLte() {
		String sql = "select id ,openid from user";
		return sql;
	}
	public static void main(String[] args) {
		FetchData fd = new FetchData("", "D:\\david_work\\david_bigdata\\mro_jdbc_hdfs\\src", 1, "mysql", ",");
		fd.execute();
	}
	
}
