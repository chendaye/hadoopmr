package org.example.hadoop.hdfs.mapcontext;

import java.util.HashMap;
import java.util.Map;

/**
 * 用map 缓存词频统计结果
 */
public class WCContext {

    // todo: 缓存容器,一个HashMap
    private Map<Object, Object> context  = new HashMap<Object, Object>();

    // getter
    public Map<Object, Object> getContext() {
        return context;
    }

    /**
     * 写数据到缓存中去
     * @param key 单词
     * @param value 次数
     */
    public void write(Object key, Object value){
        context.put(key, value);
    }

    /**
     * 从缓存中读取数据
     * @param key 单词
     * @return 单词对应的词频
     */
    public Object get(Object key){
        return context.get(key);
    }

}
