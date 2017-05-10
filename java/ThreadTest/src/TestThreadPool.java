import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class TestThreadPool {  
  
    public static void main(String[] args) {  
	
    	//需求：根据map的数量创建线程池  进行多线程 输出map中的所有元素 。
    	HashMap<Integer,Integer> map = createMap();
    	List<Integer> writeList = new ArrayList<Integer>();
    	for (Entry<Integer, Integer> entry : map.entrySet()) {
            writeList.add(entry.getKey());
        }
        ThreadPoolManager threadPoolManager = ThreadPoolManager.newInstance();  
        for (int i = 0; i < writeList.size(); i++) {  
            threadPoolManager.addExecuteTask(new MyTask(writeList.get(i),map));  
//            System.out.println("线程池中线程数目：" + threadPoolManager.getPoolSize() + "，队列中等待执行的任务数目："  
//                    + threadPoolManager.getQueue() + "，已执行玩别的任务数目：" + threadPoolManager.getCompletedTaskCount());  
        }  
        threadPoolManager.shutdown();  
        while (true) {
        	boolean end = threadPoolManager.isTaskEnd();
        	if(end == true){
        		break;
        	}
		}
        System.out.println("多线程结束");
    }  
    //创建map
    public static HashMap<Integer,Integer> createMap(){
    	HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
    	for (int i = 0; i < 100; i++) {
			map.put(i, i);
		}
    	return map;
    }
  
}  
  
//class MyTask extends Thread {  
class MyTask implements Runnable {  
    private int taskNum;  
    private HashMap<Integer,Integer> map;
    public MyTask(int taskNum,HashMap<Integer,Integer> map) {  
        this.taskNum = taskNum;  
        this.map = map;
    }  
  
    public void run() {  
        System.out.println("多线程输出的结果： " + map.get(taskNum));  
        System.out.println("task " + taskNum + "执行完毕");  
    }  
  
    public int getTaskNum() {  
        return taskNum;  
    }  
  
    public void setTaskNum(int taskNum) {  
        this.taskNum = taskNum;  
    }  
}