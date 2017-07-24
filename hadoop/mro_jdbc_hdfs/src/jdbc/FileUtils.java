package jdbc;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils {
	public void mkdir(File dir) {
		if (dir.exists()) {
			clearFile(dir);
		}
		dir.mkdirs();
	}
	
	public void mkdir(Path path) {
		try {
			FileSystem hdfs = FileSystem.get(new Configuration());
			if (hdfs.exists(path)) {
				clearFile(path);
			}
			hdfs.mkdirs(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void rename(File sourceDir, File destDir) {
		if (!sourceDir.exists()) {
			return;
		}
		sourceDir.renameTo(destDir);
	}
	
	public void rename(Path sourcePath, Path destPath) {
		try {
			FileSystem hdfs = FileSystem.get(new Configuration());
			if (!hdfs.exists(sourcePath)) {
				return;
			}
			hdfs.rename(sourcePath, destPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void clearFile(File dir) {
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				clearFile(file);
			}
		}
		dir.delete();
	}
	
	public void clearFile(Path path) {
		try {
			FileSystem hdfs = FileSystem.get(new Configuration());
			hdfs.delete(path, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized static void writeLine(Path dst,
			String data){
		FileSystem fs;
		try {
//			Configuration conf = new Configuration();
			Configuration conf = LogUtil.getConfig();
			fs = FileSystem.get(conf);
			FSDataOutputStream dos = fs.create(dst);
			dos.write(data.getBytes("UTF-8")); //writeutf出现乱码
			System.out.println(dos.getPos());
			dos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
