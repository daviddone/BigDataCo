package jdbc;

import org.apache.hadoop.conf.Configuration;

public class LogUtil {
	static boolean isTest = true;//test
//	static boolean isTest = false;//product
	public static void print(String log){
		if(isTest){
			System.out.println(log);
		}
	}
	
	public static Configuration getConfig(){
		 Configuration config = new Configuration();
		 if(isTest){
			 //本地测试
			 config.set("fs.defaultFS", "hdfs://vm10.60.0.11.com.cn:8020/user/boco/"); 
			 config.set("yarn.resourcemanager.hostname", "vm10.60.0.11.com.cn");
		 }
		 return config;
	}
}
