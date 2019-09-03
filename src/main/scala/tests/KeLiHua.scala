package tests

/**
  * Created by Kerven-HAN on 2019/9/3 15:48.
  * Talk is cheap , show me the code
  *
  * 解开科里化的面纱
  */
object KeLiHua {

  def sum(x:Int)(y:Int):Int = {x+y}

}


/*反编译之后的：
这个语法就是装十三用的感觉 如你所见，多参数不过是个虚饰，并不是编程语言的什么根本性的特质。
public final class com.coocaa.spark.test.KeLiHua {
  public static int sum(int, int);
}*/
