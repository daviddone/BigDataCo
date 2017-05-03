
import scala.collection.mutable.ArrayBuffer;

object ArrayTest {
  def main(args: Array[String]): Unit = {
    val fixed_array1 = new Array[Double](3); //定长数组
    fixed_array1(0) = 1.0 //赋值方式 按照角标赋值
    fixed_array1(1) = 2.0 
    fixed_array1(2) = 3.0 
    val fixed_array2 = Array(1, 2, 3, 4, 5);
    val fixed_array3 = Array.ofDim[Double](1,2) //定义为2维数组 
    //fixed_array3: Array[Array[Double]] = Array(Array(0.0, 0.0))
    fixed_array3(0)(0) = 1 //赋值方式
    
    val variable_array2 =new ArrayBuffer[Double](); //变长数组 和 Java中ArrayList类似
    variable_array2 += 1 
    variable_array2 += 2 
    variable_array2.insert(0, 3) //插入数据，放在位置0
    variable_array2.remove(0)  //移除掉位置为0的数据
    
    fixed_array1.foreach { x => println("fixed_array1:" + x) } 
    println("variable_array2 "+variable_array2)
    
  }
}