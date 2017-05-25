package com.david.finger.util;


public class JVMUtil {
	//取得内存使用情况
    public static String getMemoryStatus() {
        StringBuffer sb = new StringBuffer();
        sb.append("\njvm内存使用情况\n最大可用内存:");
        sb.append(Runtime.getRuntime().maxMemory());
        sb.append("\n当前JVM空闲内存:");
        sb.append(Runtime.getRuntime().freeMemory());
        sb.append("\n当前JVM占用的内存总数:");
        sb.append(Runtime.getRuntime().totalMemory());
        sb.append("\n内存使用率:");
        sb.append((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) * 100 / Runtime.getRuntime().maxMemory());
        sb.append("%");
        return sb.toString();
    }
}
