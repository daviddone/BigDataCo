public class JedisUtilsTest{
	public static void main(String[] args) {
//		JedisUtils.isTableExist("ftp_log");
//		JedisUtils.del("ftp_log");
//		JedisUtils.writeInfo("ftp_log","zhangsan1");
		JedisUtils.readInfos("ftp_log");
	}
}