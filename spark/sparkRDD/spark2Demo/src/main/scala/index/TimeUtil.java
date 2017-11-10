package index;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

/**
 * 
 * @author tangdongqing
 * @Description: 时间工具
 * @date 2017年10月24日
 */
public class TimeUtil {

	public static Date format(String formatString, String dateString) {
		try {
			return (new SimpleDateFormat(formatString)).parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	// 获取时间毫秒
	public static String getTimeToTick(String value) {
		if ("".equals(value)) {
			return "";
		}
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		try {
			return String.valueOf(df.parse(value).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}
	// 格式化时间到小时
	public static String getFormatHour(String value) {
		if(value.length()>13){
			String  hourTime = value.split(":")[0];
			hourTime = hourTime + ":00:00"; 
			return hourTime;
		}else{
			return "";
		}
	}
	public static void main(String[] args) {
		String hourTime = getFormatHour("2017-08-08 12:01:01.111");
		System.out.println(hourTime);
	}
}
