package org.example.hadoop.mr.wc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

/**
 * 使用MapperReduce 统计HDFS上的词频
 *
 * Driver: 配置Mapper Reduce 相关属性
 *
 * 提交到本地运行：在开发过程中使用的
 */
public class WordCountApp {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://master:9000");
        // 创建一个job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数：Mapper  Reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 添加Combiner设置,聚合操作与Reduce是一样的
        job.setCombinerClass(WordCountReducer.class);

        // 设置Job对应参数：Map 输入输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应参数： Reduce输入输出参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果输出目录已经存在要先删除，否则会报重复错误
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), configuration, "hadoop");
        Path outPath = new Path("/wordcount/ouput");
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath, true);  // 如果输出目录已经存在，递归删除
        }



        // 设置Job对应参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/user/hadoop/wc.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/wordcount/output2/"));

        // 提交job作业
        boolean result = job.waitForCompletion(true);

        // 提交是否成功
        System.exit(result ? 0 : -1);
    }
}
