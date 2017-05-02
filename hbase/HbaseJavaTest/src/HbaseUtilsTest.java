
import java.util.Map;

public class HbaseUtilsTest {

    public static void main(String[] args) throws Exception {

//        Map<String, String> row = HbaseUtils.getRow("user", "zhangsan", "info", "name", "age", "addr");
//        System.out.println(row);
    	HbaseUtils.createTab("tab1", "f1");
    }

}
