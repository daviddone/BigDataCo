package com.boco.wangyou.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MvHdfsUtil {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);//fixe  Filesystem closed
		
//		String tempPath = "wangyou/ltemro/xml/temp_base";
//		String sourcePath = "wangyou/ltemro/xml/temp_base/20170604/tdl_mro_basetable";
//		String targetPath = "wangyou/ltemro/xml/0000003-170717182524003-oozie-oozi-W/20170604/tdl_mro_basetable";
		String tempPath = args[0];
		String sourcePath = args[0]+"/"+args[2].replace("-", "")+"/tdl_mro_basetable";
		String targetPath = args[1]+"/"+args[2].replace("-", "")+"/tdl_mro_basetable";
		System.out.println(tempPath);
		System.out.println(sourcePath);
		System.out.println(targetPath);
		mvResultDirs(new Path(sourcePath), new Path(targetPath), conf);//移动文件
		rmHdfsDir(new Path(tempPath), conf);//删除临时路径
	}
	
	/**
     * 删除这些目录
     * 
     * @param paths
     * @throws IOException
     */
    public static void rmHdfsDir(Path path, Configuration conf) {
    	try {
    		FileSystem fs = FileSystem.get(conf);
            fs.delete(path, true);
            fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    /**
     * 转移src目录下的所有内容到dist目录下
     * 
     * @param src
     * @param dist
     */
    public static void mvResultDirs(Path sourcePath, Path targetPath, Configuration conf) {
        try {
//            FileSystem fs = sourcePath.getFileSystem(conf);
            FileSystem fs = FileSystem.get(conf);

            fs.delete(targetPath, true);
            if (fs.exists(sourcePath)) {
                if (!fs.rename(sourcePath, targetPath)) {
                    System.out.println("Unable to rename: " + sourcePath + " to: " + targetPath);
                }
            } else if (!fs.mkdirs(targetPath)) {
                System.out.println("Unable to make directory: " + targetPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
