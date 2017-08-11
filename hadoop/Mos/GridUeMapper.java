package com.boco.gridue;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import signal.index.GridUeSourceEnum;
import signal.util.JVMUtil;
import signal.util.LogUtil;
import signal.util.StringUtil2;
import signal.util.ToolUtil;

import com.boco.cdr.domain.GridUeDomain;
import com.boco.wangyou.lte.depthcoverage.MilitaryGrid;

public class GridUeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String dest_path = null;
    private MultipleOutputs mos = null;
    Text keyText = null;
    Text valueText = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	dest_path = context.getConfiguration().get("dest_path");
    	System.out.println("dest_path："+dest_path);
        mos = new MultipleOutputs(context);
        keyText = new Text();
        valueText = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        String fileName = ((FileSplit) inputSplit).getPath().getName();
        String line = value.toString();
        String[] grids = ToolUtil.splitString(line, ",");
        String enb = grids[GridUeSourceEnum.ENODEBID.getIndex()];
        if("".equals(enb.trim())||"null".equals(enb.trim())){
        	return;
        }

        MilitaryGrid militaryGrid = new MilitaryGrid();
        Double latitude = Double.parseDouble(grids[GridUeSourceEnum.LATITUDE.getIndex()])/10000000;
        Double longitude = Double.parseDouble(grids[GridUeSourceEnum.LONGITUDE.getIndex()])/10000000;
        LogUtil.print(grids[GridUeSourceEnum.LATITUDE.getIndex()]+"latitude:"+latitude+"longitude:"+longitude);
        militaryGrid.setMilitaryGridId(latitude, longitude, 50);
        String gridId = militaryGrid.getGridId();
        String gridLatLonLDRU = militaryGrid.getGridLatLonLDRU();
        
        GridUeDomain gridUe = new GridUeDomain();
        gridUe.setGridData(gridId, gridLatLonLDRU);
        gridUe.setOriginData(grids);
        
        String m_int_id = "460-00-"+gridUe.getEnodebid()+"-"+Integer.parseInt(gridUe.getEci())%256;//直接采用cgi=enb+cid/eci
        String locate_category = "0";//定位类型，指纹库是1 ，其他是0
        long msisdn = StringUtil2.EncryptStringToLong(gridUe.getImsi());
        
        gridUe.setM_int_id(m_int_id);
        gridUe.setLocate_category(locate_category);
        gridUe.setMsisdn(msisdn+"");
        
        String newLine = gridUe.toString();

        String file = dest_path + "/" + gridUe.getEnodebid()+"/";
        
        //test for new object begin###########
//        String file = dest_path + "/" + 123+"/123";
//        mos.write(NullWritable.get(),new Text(line),file);
        //test for new object end###########
        
        LogUtil.print("file:"+file);
//        mos.write(NullWritable.get(),new Text(newLine),file);
        keyText.set(file);
        value.set(newLine);
        context.write(keyText,value );
        militaryGrid = null;
        gridUe = null;
        if(JVMUtil.isDanger()){
        	System.out.println("getMemoryStatus:"+JVMUtil.getMemoryStatus());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
    
    /*1 注册失败话单
    2 呼叫失败话单
    3 掉话话单
    4 ESRVCC切换失败话单*/
    public static int getType(String localFile) {
        if (localFile.contains("cdr_zc")) {
            return 1;
        } else if (localFile.contains("cdr_fail")) {
            return 2;
        } else if (localFile.contains("cdr_drop")) {
            return 3;
        }else if(localFile.contains("cdr_esrvcc")){
        	return 4;
        }
        return 0;
    }

}
