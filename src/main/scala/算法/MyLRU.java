package 算法;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Kerven-HAN on 2019/9/5 15:27.
 * Talk is cheap , show me the code
 *
 * ref : https://blog.csdn.net/nakiri_arisu/article/details/79205660
 *
 * https://blog.csdn.net/u012969732/article/details/48434905
 */
public class MyLRU<K,V> extends LinkedHashMap<K,V> {

    private int cacheSize;

    public MyLRU(int cacheSize) {
//那个f如果不加  就是double类型，然后该构造没有该类型的入参。 然后最为关键的就是那个入参 true
        super(16, 0.75f, true);
        this.cacheSize = cacheSize;
    }


    //重写LinkedHashMap原方法
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size()>cacheSize;
    }

    public static void main(String[] args) {

        MyLRU<Integer,String> cache = new MyLRU<Integer,String>(4);
   /*     cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        cache.put(4, "four");
        cache.put(2, "two");
        cache.put(3, "three");*/

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        cache.put(4, "four");
        cache.put(5, "five");
        cache.put(6, "six");
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(1, "three");
        cache.put(4, "four");
        cache.put(4, "five");
        cache.put(3, "six");

        Iterator<Map.Entry<Integer,String>> it = cache.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<Integer, String> entry = it.next();
            Integer key = entry.getKey();
            System.out.print("Key:\t"+key);
            String Value = entry.getValue();  //这个无需打印...
            System.out.println();
        }

    }
}
