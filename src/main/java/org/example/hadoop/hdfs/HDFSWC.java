package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.example.hadoop.hdfs.mapcontext.WCContext;
import org.example.hadoop.hdfs.mapcontext.WordCount;
import org.example.hadoop.hdfs.parms.Constants;
import org.example.hadoop.hdfs.parms.ParamsUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 使用hdfs实现 word count，将结果写入hdfs
 *
 * 功能拆解：
 * 读取HDFS上的文件 ==> HDFS API
 * 业务处理：对文件的每一行进行处理  ==> Mapper
 * 将处理的每一个结果缓存起来 ==> Context
 * 将结果输出到HDFS  ==> HDFS API
 *
 * 总结：
 * 1）获取hdfs里的输入文件
 * 2）读取输入文件流，用自定义map context 处理 保存每一行信息
 * 3）读取context里保存的结果写入hdfs
 * 4)关闭资源
 * 5）配置信息写在一个文件中，以类的形式加载，避免硬编码
 */
public class HDFSWC {
    public static void main(String[] args) throws Exception {
        //todo: 连接HDFS,读取HDFS上的文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        // 获取配置文件类
        Properties properties = ParamsUtils.getProperties();
        // 获取hdfs文件系统
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), configuration, properties.getProperty(Constants.HDFS_USER));
        // 获取输入文件列表
        Path src = new Path(properties.getProperty(Constants.INPUT_PATH));
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(src, false); // 文件列表

        // todo:通过反射创建  map 对象,实现可插拔业务;果缓存类, 用自定义键值对 map 来保存 (单词, count)
//         WordCount map = new WordCount();
        Class<?> cla = Class.forName(properties.getProperty(Constants.MAP_CLASS));
        WordCount map = (WordCount)cla.newInstance();
        WCContext wcContext = new WCContext();

        // todo: 循环获取到的文件列表
        /**
         * BufferReader的作用是为其它Reader提供缓冲功能。
         * 创建BufferReader时，我们会通过它的构造函数指定某个Reader为参数。
         * BufferReader会将该Reader中的数据分批读取，每次读取一部分到缓冲中；
         * 操作完缓冲中的这部分数据之后，再从Reader中读取下一部分的数据
         */
        while (remoteIterator.hasNext()){
            // 获取每一个文件;打开获取到的文件，得到输入流;由输入流得到 BufferedReader
            LocatedFileStatus file = remoteIterator.next();
            FSDataInputStream fsDataInputStream = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line = "";
            while ((line = reader.readLine()) != null){
                // 统计词频, 把统计结果写到 wcContext
                map.map(line, wcContext);
            }
            // 关闭资源
            reader.close();
            fsDataInputStream.close();
        }


        // todo: 结果(contextMap)写到HDFS；结果集，转化为set
        Map<Object, Object> contextMap = wcContext.getContext();
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();

        // 结果存放目录,遍历集合
        Path dst = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dst,properties.getProperty(Constants.OUTPUT_FILE)));
        for (Map.Entry<Object, Object> entry : entries){
            fsDataOutputStream.writeUTF(entry.getKey().toString() +"\t"+ entry.getValue()+"\n");
        }
        // 关闭资源
        fsDataOutputStream.close();
        fileSystem.close();

        System.out.println("HDFS WC 成功！");
    }
}
