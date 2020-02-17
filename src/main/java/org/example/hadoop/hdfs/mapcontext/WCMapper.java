package org.example.hadoop.hdfs.mapcontext;

/**
 * 自定义Mapper
 */
public interface WCMapper {
    /**
     *
     * @param line 读取每一行数据
     * @param context 上下文/缓存
     */
    public void map(String line, WCContext context);
}
