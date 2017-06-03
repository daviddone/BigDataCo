package com.david.es;

import com.david.es.bean.Info;


public class esUtilTest 
{
    public static void main( String[] args )
    {
    	esUtil.get();
    	String index = "cq";
    	String type = "info";
    	Info info = new Info("标题 测试1", "内容描述", "www.test.com");
    	String id = esUtil.addIndexByObject(index, type, info);
    	System.out.println(id);
    }
}
