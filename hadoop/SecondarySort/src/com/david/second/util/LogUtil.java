package com.david.second.util;

import org.apache.hadoop.conf.Configuration;
/**
 * 
 * @author tangdongqing
 * @Description: 本地测试和日志开关
 * @date 2017年10月24日
 */
public class LogUtil {
//	static boolean isTest = false;//product
//	static boolean isLocal = false;//product
	static boolean isTest = true;//test 是否输出日志
	static boolean isLocal = true;//test 是否本地测试
	public static void print(String log){
		if(isTest){
			System.out.println(log);
		}
	}
	
	public static Configuration getConfig(){
		 Configuration config = new Configuration();
		 if(isLocal){
			 //本地测试
			 config.set("fs.defaultFS", "hdfs://10.50.15.103:9000/"); 
//			 config.set("yarn.resourcemanager.hostname", "david");
		 }
		 return config;
	}
}
