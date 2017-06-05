package com.david.es;

import java.util.Map;

import com.david.es.bean.Info;


public class esUtilTest 
{
    public static void main( String[] args )
    {
    	esUtil.get();
    	String index = "cq";
    	String type = "info";
    	Info info = new Info("张三丰", "内容描述", "www.test.com");
    	String id = esUtil.addIndexByObject(index, type, info);
    	System.out.println(id);
    	Map<String, Object> map = esUtil.search("张三丰", index, type, 0, 5);
    	System.out.println(map.toString());
    }
}
