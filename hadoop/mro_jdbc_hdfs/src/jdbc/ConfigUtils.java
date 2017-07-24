package jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConfigUtils {
	public static Properties getConfig(String cfgFile) {
		InputStreamReader inputStream = null;
		Properties prop = new Properties();
		try {
			inputStream = new InputStreamReader(new FileInputStream(cfgFile), "UTF-8");
			prop.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
	public Properties getHdfsConfig(String cfgFile) {
		InputStreamReader inputStream = null;
		Properties prop = new Properties();
		Path path = new Path(cfgFile);
		try {
			FileSystem fs = path.getFileSystem(new Configuration());
			inputStream = new InputStreamReader(fs.open(path), "UTF-8");
			prop.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
}
