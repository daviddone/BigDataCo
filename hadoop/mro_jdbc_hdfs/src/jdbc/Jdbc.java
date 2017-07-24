package jdbc;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class Jdbc {
	private String _dbType = "";
	private ConfigConstants _dbCfgInfo = null;
	private String _splitStr = "";
	private Connection con = null;
	private Statement stmt = null;
	private Base64 base64 = new Base64();
	
	public Jdbc(String cfgDir, String dbType, int type, String splitStr) {
		_dbType = dbType.toLowerCase();
		_dbCfgInfo = new ConfigConstants(cfgDir + "/" + _dbType + "_config.properties", type);
		_splitStr = splitStr;
	}
	
	public void connect() {
		try {
			if ("informix".equals(_dbType)) {
				Class.forName(_dbCfgInfo.getValue("informix_driver", "")).newInstance();
				String url = _dbCfgInfo.getValue("informix_jdbc", "") + ":informixserver=" + _dbCfgInfo.getValue("informix_server", "")
						+ ";database=" + _dbCfgInfo.getValue("database", "") + ";user=" + _dbCfgInfo.getValue("db_user", "")
						+ ";password=" + base64.decode(_dbCfgInfo.getValue("db_password", "")) + ";" + _dbCfgInfo.getValue("db_code", "")
						+ ";IFX_USE_STRENC=true";
				con = DriverManager.getConnection(url);
			} else if ("oracle".equals(_dbType)) {
				Class.forName(_dbCfgInfo.getValue("oracle_driver", "")).newInstance();
				String url = _dbCfgInfo.getValue("oracle_jdbc", "") + ":" + _dbCfgInfo.getValue("oracle_sid", "") + _dbCfgInfo.getValue("db_code", "");
				con = DriverManager.getConnection(url, _dbCfgInfo.getValue("db_user", ""), base64.decode(_dbCfgInfo.getValue("db_password", "")));
			} else if ("mysql".equals(_dbType)) {
				Class.forName(_dbCfgInfo.getValue("mysql_driver", "")).newInstance();
//				Class.forName("com.mysql.jdbc.Driver");
				String url = _dbCfgInfo.getValue("mysql_jdbc", "")+"/"+_dbCfgInfo.getValue("database", "");
				String userName = _dbCfgInfo.getValue("db_user", "");
				String pwd = _dbCfgInfo.getValue("db_password", "");
				System.out.println(userName+pwd+url);
				con = DriverManager.getConnection(url, userName, pwd);
			} 
			stmt = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void executeSql(String sql, String fileName) {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, false)));
			ResultSet rs = stmt.executeQuery(sql);
			int cnt = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				StringBuffer line = new StringBuffer(rs.getString(1));
				for (int i = 2; i <= cnt; i++) {
					line.append(_splitStr);
					if (rs.getString(i) != null) {
						line.append(rs.getString(i));
					}
				}
				bw.write(line.toString() + "\n");
			}
			rs.close();
			bw.flush();
			bw.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String executeSql(String sql) {
		StringBuffer sb = new StringBuffer();
//		List<String> lines = new ArrayList<String>();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			int cnt = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				StringBuffer line = new StringBuffer(rs.getString(1));
				for (int i = 2; i <= cnt; i++) {
					line.append(_splitStr);
					if (rs.getString(i) != null) {
						line.append(rs.getString(i));
					}
				}
//				lines.add(line.toString());
				sb.append(line.toString() + "\n");
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public void close() {
		try {
			if (stmt != null) {
				stmt.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
