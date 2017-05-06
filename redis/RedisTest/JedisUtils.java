
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class JedisUtils {
	private static String host;
	private static int port;
	private static String AUTH;
	private static JedisPoolConfig conf = null;
	private static JedisPool pool;
	private static Pattern pattern = Pattern.compile("\\^\\^");

	private static void initPool() {
		conf = new JedisPoolConfig();
		conf.setMaxActive(1500);
		conf.setMaxIdle(1000);
		conf.setMaxWait(15000);
		conf.setTestOnBorrow(true);
		host = "10.60.0.11";
		port = 6379;
		AUTH = "";
		if(!AUTH.equals("")){
			pool = new JedisPool(conf, host, port, 60 * 1000, AUTH);
		}else{
			pool = new JedisPool(conf, host, port, 60 * 1000);
		}
	}

	private static Jedis getJedis() {
		if (null == pool) {
			initPool();
		}
		return pool.getResource();
	}

	private static void returnJedis(Jedis jedis) {
		if (jedis != null) {
			pool.returnResource(jedis);
		}
	}

	private static void returnBrokenJedis(Jedis jedis) {
		if (jedis != null) {
			pool.returnBrokenResource(jedis);
		}
	}

	public static void closeRedis(Jedis jedis) {
		if (jedis != null) {
			jedis.disconnect();
			pool.destroy();
		}
		conf = null;
	}

	/**
	 * 将文件的内容初始化到REDIS中
	 * @param tableName 表名
	 * @param fileName 文件的名字
	 */
	public static void initRedisTable(String tableName, String fileName) {
		File file = null;
		file = new File(fileName);
		BufferedReader in = null;
		Jedis jedis = null;
		Pipeline pipe = null;
		int count = 0;
		try {
			in = new BufferedReader(new FileReader(file));
			jedis = getJedis();
			pipe = jedis.pipelined();
			if (jedis.exists(tableName)) {
				jedis.del(tableName);
			}
			String line;
			while ((line = in.readLine()) != null) {
				pipe.lpush(tableName, line);
				count++;
			}
			pipe.sync();
			returnJedis(jedis);
			System.out.printf("finish init:%s\n", tableName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			returnBrokenJedis(jedis);
		} finally {
			System.out.printf("insert %d\n", count);
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 根据表名和主键的名字，取出来一个HASH
	 * @param tableName 表名
	 * @param keyStr 主键的名字
	 * @return List<String>
	 */
	public static List<String> getTabHashValues(String tableName,String keyStr) {
		Jedis jedis = null;
		List<String> value = new ArrayList<String>();
		jedis = getJedis();
		// 如果redis中没有制定的表
		if (!jedis.exists(tableName)) {
			// 初始化 这张表
			System.out.println("No " + tableName + " ini for this city!");
		} else if(!jedis.hexists(tableName,keyStr)){
			System.out.println("NO ENBID"+keyStr+" AT "+ tableName + " ini!");
		}else{
			String[] thisStrs = pattern.split(jedis.hget(tableName, keyStr));
			for(String thisStr:thisStrs)			
				value.add(thisStr);			
			returnJedis(jedis);
		}
		return value;
	}
	/**
	 * 将HashMap<String,List<String>> 的值初始化到REDIS中	 
	 * @param tableName 表的名字
	 * @param tabHashMap HashMap<String,List<String>>
	 */
	public static void initRedisHashTable(String tableName,
			Map<String, List<String>> tabHashMap) {
		Jedis jedis = null;
		int count = 0;
		try {
			jedis = getJedis();
			if (jedis.exists(tableName)) {
				jedis.del(tableName);
			}
			for (Iterator<String> itr = tabHashMap.keySet().iterator(); itr
					.hasNext();) {
				String mapKey = itr.next();
				List<String> thisList = tabHashMap.get(mapKey);

				StringBuffer sb = new StringBuffer();
				sb.setLength(0);
				for (int i = 0; i < thisList.size(); i++) {
					if (i == 0)
						sb.append(thisList.get(i));
					else
						sb.append("^^").append(thisList.get(i));
					count++;
				}
				jedis.hset(tableName, mapKey, sb.toString());
			}
			returnJedis(jedis);
			System.out.printf("finish init:%s\n", tableName);
		} catch (Exception e) {
			e.printStackTrace();
			returnBrokenJedis(jedis);
		} finally {
			System.out.printf("insert %d\n", count);
		}
	}

	/**
	 * 
	 * @param tableName
	 * @return list the values of the table
	 */
	public static List<String> getTableValues(String tableName) {
		Jedis jedis = null;
		List<String> value = new ArrayList<String>();
		jedis = getJedis();
		// 如果redis中没有制定的表
		if (!jedis.exists(tableName)) {
			// 初始化 这张表
			System.out.println("No " + tableName + " ini for this city!");
		} else {
			value = jedis.lrange(tableName, 0, -1);
			returnJedis(jedis);
		}
		return value;
	}
	
	/**
	 * 判断该表是否存在
	 * @param tableName 表名
	 * @return 存在true,不存在false
	 */
	public static boolean isTableExist(String tableName){
		boolean flag = true;
		Jedis jedis = getJedis();
		if (!jedis.exists(tableName)) {
			System.out.println("No " + tableName + " ini for this city!");
			flag = false;
		}		
		return flag;
	}

	public static void putOneData2RedisLb(String key, String value) {
		Jedis jedis = getJedis();
		jedis.set(key, value);
		returnJedis(jedis);
	}

	public static String getOneData2RedisLb(String key) {
		Jedis jedis = getJedis();
		String s = jedis.get(key);
		returnJedis(jedis);
		return s;
	}

	/**
	 * 根据key删除表
	 * 
	 * @param key
	 */
	public static void del(String... key) {
		Jedis j = getJedis();
		j.del(key);
		pool.returnResource(j);
	}
	
	
	public static void writeInfo(String tableName,String eachInfo) {
		Jedis jedis = getJedis();
		Pipeline pipe = jedis.pipelined();
		pipe.lpush(tableName, eachInfo);
		pipe.sync();
		returnJedis(jedis);
	}
	

	public static void readInfos(String tableName) {
		Jedis jedis = getJedis();
		StringBuffer sb = new StringBuffer();
		List<String> list = jedis.lrange(tableName,0,-1);
			for(int i=0; i<list.size(); i++) {
				sb.append(list.get(i)+"\n");
			}
		System.out.println(jedis.llen(tableName));
		System.out.println(sb.toString());
//	    System.out.println("服务器信息："+jedis.info());
	}
}
