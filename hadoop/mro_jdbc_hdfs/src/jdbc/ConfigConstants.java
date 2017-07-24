package jdbc;

import java.util.ArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConfigConstants {
	private Properties propertiesFile = null;
	//ftp重连次数 每次间隔3秒
	public final static int  FTP_RECONNECT_NUM =2;
	public final static  int FILE_SIZE = 10;
	
	//0 本地文件  ,1 hdfs文件
	public ConfigConstants(String cfgFile, int type) {
		ConfigUtils configUtils = new ConfigUtils();
		if (type == 0) {
			propertiesFile = configUtils.getConfig(cfgFile);
		} else {
			propertiesFile = configUtils.getHdfsConfig(cfgFile);
		}
	}
	
	public String getValue(String key, String defaultVal) {
		return propertiesFile.getProperty(key, defaultVal);
	}
	
	public String getValue(String key) {
		return propertiesFile.getProperty(key);
	}
	
	public List<Integer> getHours(String key) {
		String[] hours = propertiesFile.getProperty(key, "").split(",", -1);
		List<Integer> hourList = new ArrayList<Integer>();
		int len = hours.length;
		for (int i = 0; i < len; i++) {
			if (!"".equals(hours[i])) {
				hourList.add(Integer.parseInt(hours[i]));
			}
		}
		return hourList;
	}
	
	public List<String> getVariableTabs(String key) {
		String[] variableTabs = propertiesFile.getProperty(key, "").toLowerCase().split("\\|", -1);
		return Arrays.asList(variableTabs);
	}
}
