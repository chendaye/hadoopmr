package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 使用Java API 操作HDFS文件系统
 * */
public class HDFSApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        /**
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数：HDFS的URI
         * 第二个参数：客户端指定的配置参数
         * 第三个： 客户端的身份。即用户名
         *
         * */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), configuration, "hadoop");
        Path path = new Path("hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }
}
