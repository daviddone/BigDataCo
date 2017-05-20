import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class TestThreadPool {  
  
    public static void main(String[] args) throws InterruptedException {  
	
    	//需求：根据map的数量创建线程池  进行多线程 输出map中的所有元素 。
    	HashMap<Integer,Integer> map = createMap();
    	List<Integer> writeList = new ArrayList<Integer>();
    	for (Entry<Integer, Integer> entry : map.entrySet()) {
            writeList.add(entry.getKey());
        }
    	int thread_num = 2;
        ThreadPoolManager threadPoolManager = ThreadPoolManager.newInstance();  
        for (int i = 0; i < thread_num; i++) {  
            threadPoolManager.addExecuteTask(new MyTask(writeList,map,i,thread_num));  
            //System.out.println("线程池中线程数目：" + threadPoolManager.getPoolSize() + "，队列中等待执行的任务数目："  
             //       + threadPoolManager.getQueue() + "，已执行玩别的任务数目：" + threadPoolManager.getCompletedTaskCount());  
        }  
        threadPoolManager.shutdown();  
        while (true) {
        	if(threadPoolManager.isEnd()){
        		System.out.println("1号线程池 执行完毕");
        		break;
        	}
        	Thread.sleep(200);
		}
        System.out.println("1号多线程结束");
        threadPoolManager = null;
        ThreadPoolManager threadPoolManager1 = ThreadPoolManager.newInstance();  
        for (int i = 0; i < thread_num; i++) { 
        		threadPoolManager1.addExecuteTask(new MyTask(writeList,map,i,thread_num));  
        }  
        threadPoolManager1.shutdown();  
        while (true) {
        	if(threadPoolManager1.isEnd()){
        		System.out.println("2号线程池执行完毕 ");
        		break;
        	}
        	Thread.sleep(200);
		}
        System.out.println("2号多线程结束");
    }  
    //创建map
    public static HashMap<Integer,Integer> createMap(){
    	HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
    	for (int i = 0; i < 9; i++) {
			map.put(i, i);
		}
    	return map;
    }
  
}  
  
//class MyTask extends Thread {  
class MyTask implements Runnable {  
    private List<Integer> lists;  
    private HashMap<Integer,Integer> map;
    private int thread_num;
    private int max_thread_num;
    public MyTask(List<Integer> lists,HashMap<Integer,Integer> map,int thread_num,int max_thread_num) {  
        this.lists = lists;  
        this.map = map;
        this.thread_num = thread_num;
        this.max_thread_num = max_thread_num;
    }  
  
    public void run() {  
    	for (int i = 0; i < lists.size(); i++) {
    		if (i % max_thread_num != thread_num)
    			continue;
    		 System.out.println("多线程输出的结果： " + map.get(i));  
//    	     System.out.println("task " + thread_num + "执行完毕");  
		}
       
    }  
  
}