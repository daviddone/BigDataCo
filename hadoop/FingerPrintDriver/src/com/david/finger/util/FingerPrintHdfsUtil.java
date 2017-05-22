package com.david.finger.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FingerPrintHdfsUtil {
	private static FileSystem fs;
	private static BufferedReader br = null;
	
	public static void main(String[] args) throws IOException {
	
	}
    
	public static InputStream getStream(String path) {
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(path));
            return in;
        } catch (IOException e) {
        	
        }
        return in;
    }
	@SuppressWarnings("unchecked")
	public static void writeFinger(List<String> lines,String destPath,Text result,MultipleOutputs<Text, NullWritable> mos,String enb) throws IOException, InterruptedException {
		List<FingerBean> cellRsrpList = null;
		Map<String, List<FingerBean>> ScRsrpMap = new HashMap<String, List<FingerBean>>();
		System.out.println("file begin:");
		for (String line:lines) {
			System.out.println(line);
			String[] datas = line.split(";");
			if(datas.length<6){
				continue;
			}
			List<NcCellRsrp> ncCellRsrpList = new ArrayList<NcCellRsrp>();
			FingerBean finger = new FingerBean();
			StringBuffer gridInfoSb = new StringBuffer();
			String scRsrpGridKey = "";
			for (int i = 0; i < datas.length; i++) {
				if(i>=0 && i < 6){
					gridInfoSb.append(datas[i]+ ";");
				}
				if(i == 6){ //第7个是 主小区
					String scEnbId = datas[i].split(",")[0];
					String scCellId = datas[i].split(",")[1];
					int scRsrp = Integer.parseInt(datas[i].split(",")[2]);
					finger.setScEnbId(scEnbId);
					finger.setScCellId(scCellId);
					finger.setScRsrp(scRsrp);
				}
				if(i > 6){ //第8个以后是邻区
					String ncEnbId = datas[i].split(",")[0];
					String ncCellId = datas[i].split(",")[1];
					int ncRsrp = Integer.parseInt(datas[i].split(",")[2]);
					if(ncRsrp < -120){
						continue;//抛掉邻区 rsrp微弱 对应的  单个邻区的数据 
					}else{
						NcCellRsrp ncCellRsrp = new NcCellRsrp(ncEnbId,ncCellId,ncRsrp);
						ncCellRsrpList.add(ncCellRsrp);
					}
				}
			}
			if(finger.getScRsrp()<-120){
				continue; //抛掉主小区 rsrp微弱 对应的整条 数据
			}
			String gridInfoStr = gridInfoSb.toString().substring(0,gridInfoSb.length()-1);
			scRsrpGridKey = gridInfoStr+";"+finger.getScEnbId()+","+finger.getScCellId();
			finger.setGridInfo(gridInfoStr);
			finger.setNcCellRsrpList(ncCellRsrpList);
			if(ScRsrpMap.containsKey(scRsrpGridKey)){
				cellRsrpList = ScRsrpMap.get(scRsrpGridKey);
			}else{
				cellRsrpList = new ArrayList<FingerBean>();
			}
			cellRsrpList.add(finger);
			ScRsrpMap.put(scRsrpGridKey, cellRsrpList);
		}
		ScRsrpComparator scRsrpComparator = new ScRsrpComparator();
		//将Map转化为List集合，List采用ArrayList  
        List<Map.Entry<String, List<FingerBean>>> list_Data = new ArrayList<Map.Entry<String,List<FingerBean>>>(ScRsrpMap.entrySet());  
		Collections.sort(list_Data, scRsrpComparator);
		int scLength = (list_Data.size()>6)?6:list_Data.size();
	 	for(int i=0 ; i<scLength ;i++){ 
	 		 Map.Entry<String, List<FingerBean>> map = list_Data.get(i);
             System.out.println(map.getKey()+":"+map.getValue()); //对第一小区，对其邻区进行计数和排序。
             List<FingerBean> ncRsrpList = map.getValue();
             Map<String, NcCellRsrpSortBean> ncRsrpSumMap = new HashMap<String, NcCellRsrpSortBean>();
             int scRsrpSum = 0;
             for (FingerBean f : ncRsrpList) {
            	 List<NcCellRsrp> ncCellList = f.getNcCellRsrpList();
            	 scRsrpSum = scRsrpSum + f.getScRsrp();
            	 int ncRsrpCnt = 0;
            	 int ncRsrpSum = 0;
            	 double ncRsrpAvg = 0.0;
 				 for (NcCellRsrp ncCellRsrp : ncCellList) {
 					String ncRsrpKey = ncCellRsrp.getEnb_id()+","+ncCellRsrp.getCellid();
 					if(ncCellRsrp.getRsrp()<-120){
 						continue;
 					}
 					if(ncRsrpSumMap.containsKey(ncRsrpKey)){
 						ncRsrpCnt = ncRsrpSumMap.get(ncRsrpKey).getNcRsrpCnt()+1;
 						ncRsrpSum = ncRsrpSumMap.get(ncRsrpKey).getNcRsrpSum()+ncCellRsrp.getRsrp();
 					}else{
 						ncRsrpCnt = 1;
 						ncRsrpSum = ncCellRsrp.getRsrp();
 					}
 					ncRsrpAvg = ncRsrpSum/ncRsrpCnt;
 					NcCellRsrpSortBean ncSortBean = new NcCellRsrpSortBean(ncRsrpSum, ncRsrpCnt, ncRsrpAvg);
 					ncRsrpSumMap.put(ncRsrpKey, ncSortBean);
 				}
             }
             
             NcRsrpCntComparator ncRsrpComparator = new NcRsrpCntComparator();//按照邻区rsrp数量排序
             List<Map.Entry<String, NcCellRsrpSortBean>> nc_list_Data = new ArrayList<Map.Entry<String,NcCellRsrpSortBean>>(ncRsrpSumMap.entrySet());  
     		 Collections.sort(nc_list_Data, ncRsrpComparator);
     		 int ncLength = (nc_list_Data.size()>9)?9:nc_list_Data.size();
     		 StringBuffer ncLine = new StringBuffer();
     		 for(int j=0 ; j<ncLength ;j++){ //邻区rsrp 最多的9个
     			 Map.Entry<String, NcCellRsrpSortBean> ncmap = nc_list_Data.get(j);
                 double ncRsrpAvg = ncmap.getValue().getNcRsrpAvg();
                 ncLine.append(ncmap.getKey()+","+ncRsrpAvg+";");
                 System.out.println("###"+ncLine.toString());
     		 }
     		 
	 		 StringBuffer sbLine = new StringBuffer();
	 		 sbLine.append(map.getKey() + ",");//栅格数据gridInfo + 主小区enb_id cellid
	 		 sbLine.append(scRsrpSum/ncRsrpList.size()+";");//scrsrp
	 		 sbLine.append(ncLine.toString());//ncrsrp
	 		 String sbLineStr = sbLine.toString();
//	 		 String sbLineStr = sbLine.toString().substring(0, sbLine.length()-1); //移除最后分号
	 		 //输出数据
	 		 result.set(sbLineStr);
//	 		 context.write(result, NullWritable.get());
	 		 mos.write(result, NullWritable.get(), destPath+"/"+enb+"/");
        } 
	}
	
	private static class ScRsrpComparator implements Comparator<Map.Entry<String,List<FingerBean>>> {
		@Override
		public int compare(Entry<String, List<FingerBean>> f1,
                Entry<String, List<FingerBean>> f2) {
			if(f1.getValue().size() < f2.getValue().size()){
				return 1;
			}else if(f1.getValue().size() == f2.getValue().size()){
				return 0;
			}else{
				return -1;
			}
		}

	}
	//按照邻区rsrp数量进行排序
	private static class NcRsrpCntComparator implements Comparator<Map.Entry<String,NcCellRsrpSortBean>> {
		@Override
		public int compare(Entry<String,NcCellRsrpSortBean> f1,
				Entry<String,NcCellRsrpSortBean> f2) {
			if(f1.getValue().getNcRsrpCnt() < f2.getValue().getNcRsrpCnt()){
				return 1;
			}else if(f1.getValue().getNcRsrpCnt() == f2.getValue().getNcRsrpCnt()){
				return 0;
			}else{
				return -1;
			}
		}

	}
	
}
