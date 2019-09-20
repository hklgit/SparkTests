package 算法;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Kerven-HAN on 2019/9/19 17:35.
 * Talk is cheap , show me the code
 */
public class MyLRUv2<K,V> extends LinkedHashMap<K,V> {

    private int cacheSize;

    public MyLRUv2(int cacheSize){
        super(16,0.75f,true);
        this.cacheSize = cacheSize;

    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest){

        return size() > cacheSize;

    }


}
