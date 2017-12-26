package com.david.second.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Tools {

    /*
     * 修改文件名
     */
    public static void rename(String oldFile, String newFile) {
        File file = new File(oldFile);
        if (file.exists()) {
            file.renameTo(new File(newFile));
        }
    }

    /*
     * 创建文件或目录
     */
    public static void mkdir(String dirName) {
        File dir = new File(dirName);
        if (dir != null) {
            if (dir.exists()) {
                clearFile(dirName);
            }
            dir.mkdirs();
        }
    }

    /*
     * 删除文件或目录
     */
    public static void clearFile(String fileName) {
        File dir = new File(fileName);
        if (dir.isDirectory()) {
            String[] files = dir.list();
            for (String file : files) {
                clearFile(fileName + "/" + file);
            }
        }
        dir.delete();
    }


    /*
     * 格式化日期
     *
     * form:1458666092461
     * to:2016-03-23 01:01:32.461
     */
    public static String getDateTime(long value) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return df.format(value);
    }

    /*
     * 格式化日期
     *
     * form:1484027100321
     * to:2017-01-10 13:00:00
     */
    public static String getDateHour(long value) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        return df.format(value);
    }


    /*
     * 格式化日期
     *
     * form:20160323010132461
     * to:2016-03-23 01:01:32.461
     */
    public static String convertDate(String value) {
        if ("".equals(value)) {
            return "";
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        DateFormat df2 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        try {
            return df.format(df2.parse(value).getTime());
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }


    /*
     * 格式化日期
     *
     * form:1484016247282
     * to:20170110104407282
     */
    public static String getMilliDate(String value) {
        if ("".equals(value)) {
            return "";
        }
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return df.format(Long.parseLong(value));
    }


    /*
     * 格式化日期
     *
     * form:1458666092461
     * to:2016032301
     */
    public static String getHour(String value) {
        if ("".equals(value)) {
            return "";
        }
        DateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(Long.parseLong(value));
    }

    /*
     * 时间差
     */
    public static long diffHour(String dt1, String dt2) {
        DateFormat df = new SimpleDateFormat("yyyyMMddHH");
        try {
            long tick1 = df.parse(dt1).getTime();
            long tick2 = df.parse(dt2).getTime();
            return Math.abs(tick1 - tick2) / 1000;//得到秒数
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 99999;
    }

    /*
     * 转成秒
     *
     * 2016-03-23 01:01:32.461
     * 1458666092000
     */
    public static String getTick(String value) {
        if ("".equals(value)) {
            return "";
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return String.valueOf(df.parse(value).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * @param （1484016247282）
     * @return 该毫秒数转换为 * days * hours * minutes * seconds 后的格式
     * @author fy.zhang
     */
    public static String formatDuring(long mss) {
        long days = mss / (1000 * 60 * 60 * 24);
        long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
        long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
        long seconds = (mss % (1000 * 60)) / 1000;
        return days + " days " + hours + " hours " + minutes + " minutes "
                + seconds + " seconds ";
    }

    /*
     * 转成毫秒
     *
     * 20160323 010132461
     * 1458666092461
     */
    public static String getSecond(String value) {
        if ("".equals(value)) {
            return "";
        }
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        try {
            return String.valueOf(df.parse(value).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    /*
     * 十六进制转十进制
     */
    public static String hexToDec(String value) {
        if ("".equals(value)) {
            return "";
        }
        return String.valueOf(Long.parseLong(value, 16));
    }

    /**
     * 根据经纬度计算距离，单位m
     */
    public static double getDis(double lat1, double lon1, double lat2, double lon2) {
       /* if (Math.abs(lat1) < 0.000001 || Math.abs(lon1) < 0.000001
                || Math.abs(lat2) < 0.000001 || Math.abs(lon2) < 0.000001) {
            return 99999999;
        }*/
        double r = 6371.004;

        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double b = rad(lon1) - rad(lon2);

        double c = Math.sin(radLat1) * Math.sin(radLat2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.cos(b);
        return 1000 * r * Math.acos(c);
    }

    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }

    /*
     * Java文件操作 获取文件扩展名
     *
     * Created on: 2011-8-2 Author: blueeagle
     */
    public static String getExtensionName(String filename) {
        if ((filename != null) && (filename.length() > 0)) {
            int dot = filename.lastIndexOf('.');
            if ((dot > -1) && (dot < (filename.length() - 1))) {
                return filename.substring(dot + 1);
            }
        }
        return filename;
    }

    /*
     * Java文件操作 获取不带扩展名的文件名
     *
     * Created on: 2011-8-2 Author: blueeagle
     */
    public static String getFileNameNoEx(String filename) {
        if ((filename != null) && (filename.length() > 0)) {
            int dot = filename.lastIndexOf('.');
            if ((dot > -1) && (dot < (filename.length()))) {
                return filename.substring(0, dot);
            }
        }
        return filename;
    }

    /**
     * Split the string of the specified{@code value} to string[].
     *
     * @param value the string to split
     * @return String[]
     */
    public static String[] splitString(String value, String separator) {
        List<String> subArray = new ArrayList<String>();
        while (true) {
            String splitStr = null; // 保留截取的字符串
            int index = value.indexOf(separator);
            if (index < 0) {
                break;
            }
            splitStr = value.substring(0, index);
            value = value.substring(index + 1);
            subArray.add(splitStr);
        }
        subArray.add(value);

        String[] newItems = new String[subArray.size()];
        return subArray.toArray(newItems);
    }


    /**
     * 按行读取HDFS文件
     *
     * @param hdfsFilePath  HDFS文件路径
     * @param mapContext
     * @param reduceContext
     * @param lineSize      长度
     * @return 文本集合
     * @throws IOException
     */
    public static List<String[]> readHdfsFile(String hdfsFilePath,
                                              Mapper.Context mapContext, Reducer.Context reduceContext,
                                              String separator, int lineSize) throws IOException {
        List<String[]> lineList = new ArrayList<String[]>();

        //注意：单元测试使用，发包注掉
        /*Configuration conf = new Configuration();
        conf.set("fs.default.name", "10.60.0.11");
        FileSystem fileSystem = FileSystem.get(conf);*/

        FileSystem fileSystem = null;
        if (mapContext != null) {
            fileSystem = FileSystem.get(mapContext.getConfiguration());
        } else {
            fileSystem = FileSystem.get(reduceContext.getConfiguration());
        }

        //读取hdfs上的文件
        FSDataInputStream fs = fileSystem.open(new Path(hdfsFilePath));
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));

        String line = "";
        while ((line = br.readLine()) != null) {
//            String[] lineColumns = line.split(separator, -1);//注意：需要"\\|"
            String[] lineColumns = splitString(line, separator);//注意：需要"|"

            //长度过滤
            if (lineColumns.length < lineSize) {
                continue;
            }
            lineList.add(lineColumns);
        }

        br.close();
        return lineList;
    }

    /**
     * 按行读取HDFS文件,而且封装成map
     * @param hdfsFilePath
     * @param mapContext
     * @param reduceContext
     * @param separator
     * @param lineSize
     * @return
     * @throws IOException
     */
    /*public static Map<String,String[]> readHdfsFile12(String hdfsFilePath,
                                              Mapper.Context mapContext, Reducer.Context reduceContext,
                                              String separator, int lineSize) throws IOException {
        FileSystem fileSystem = null;
        if (mapContext != null) {
            fileSystem = FileSystem.get(mapContext.getConfiguration());
        } else {
            fileSystem = FileSystem.get(reduceContext.getConfiguration());
        }

        //读取hdfs上的文件
        FSDataInputStream fs = fileSystem.open(new Path(hdfsFilePath));
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));

        Map<String,String[]> roadGridMap = new HashMap<>();

        String line = "";
        while ((line = br.readLine()) != null) {
            String[] lineColumns = splitString(line, separator);//注意：需要"|"

            //长度过滤
            if (lineColumns.length < lineSize) {
                continue;
            }

            String gridId = lineColumns[IndexRoadwayGridIniEnum.GRID_ID.getIndex()];
            if (roadGridMap.containsKey(gridId)){
                continue;
            }
            roadGridMap.put(gridId,lineColumns);
        }
        br.close();

        return roadGridMap;
    }*/


    /**
     * 获得一个[0,max)之间的随机整数。
     * @param max
     * @return
     */
    public static int getRandomInt(int max) {
        Random ran = new Random();
        int n = ran.nextInt(max);

        return n;
    }

    /**
     * 获得一个[min, max]之间的随机整数
     * @param min
     * @param max
     * @return
     */
    public static int getRandomInt(int min, int max) {
        Random ran = new Random();
        return ran.nextInt(max-min+1) + min;
    }

    /**
     * 根据时间戳获取小时
     * @param millisTime 时间戳
     * @return 小时
     */
    public static int getHourInMillis(String millisTime){
        if(millisTime!=null&&!"".equals(millisTime)){
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(millisTime));
            return calendar.get(Calendar.HOUR_OF_DAY);
        }else {
            return -1;
        }

    }
    
    /***
     * 合并hdfs上的文件，
     * 将一个文件夹下的所有文件合并到指定的文件中。
     * @param folder
     * @param file
     */
    public static void copyHdfsMerge(String folder, String file) {  
		  
	    Path src = new Path(folder);  
	    Path dst = new Path(file);  
//	    Configuration conf = new Configuration();  
	    Configuration conf = LogUtil.getConfig(); 
	    try {  
	    	
	    	//判断是否目标存在，如果存在则删除
			FileSystem fileSystem = dst.getFileSystem(conf);
			if (fileSystem.exists(dst)) {
				System.out.println("输出的目标文件已存在，删除目标文件"+file);
				fileSystem.delete(dst, true);
			}
			
	    	LogUtil.print(Calendar.getInstance().getTime()+"开始合并小文件到单个文件");
	        FileUtil.copyMerge(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf, null);  
//	    	FileUtil.copyMerge(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf, "\n");
	        LogUtil.print(Calendar.getInstance().getTime()+"完成合并小文件到单个文件");
	        
	    } catch (IOException e) {
	        // TODO Auto-generated catch block  
	        e.printStackTrace();  
	    }
	}  
    
    public static void cleanDir(String destPath){
    	Configuration conf = LogUtil.getConfig();
    	Path dest = new Path(destPath);
    	FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			if (fs.exists(dest)) {
	    		fs.delete(dest, true);
	    	}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    //795B80D
    public static String getEnbId(String hexStr){
    	String enbId = "";
    	try {
        	enbId = Integer.parseInt(hexStr.substring(0,hexStr.length()-2),16)+"";
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return enbId;
    }
    //795B80D
    public static String getCi(String hexStr){
    	String ci = "";
    	try {
//    		String hexStr = Integer.toHexString(cellId);
    		ci = Integer.parseInt(hexStr.substring(hexStr.length()-2,hexStr.length()),16)+"";
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return ci;
    }
    
    public static String getCgi(int cellId){//460-00-enbid-ci
    	String hexStr = Integer.toHexString(cellId);
    	String cgi = "";
    	try {
    		int enbId = Integer.parseInt(hexStr.substring(0,hexStr.length()-2),16);
        	int ci = Integer.parseInt(hexStr.substring(hexStr.length()-2,hexStr.length()),16);
        	cgi = "460-00-"+enbId+"-"+ci;
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return cgi;
    }
    public static void main(String[] args) {
//		System.out.println(getCgi(112366083))i
		System.out.println(getEnbId("795B80D"));
		System.out.println(getCi("795B80D"));
	}
}



