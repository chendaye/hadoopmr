package org.example.hadoop.mr.wc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

/**
 * 使用MapperReduce 统计HDFS上的词频
 *
 * Driver: 配置Mapper Reduce 相关属性
 *
 * 提交到本地运行：在开发过程中使用的
 *
 *
 * 使用本地数据源，结果输出到本地
 *
 * 在开发过程中使用本地模式进行开发
 * 然后在服务器进行调优
 */
public class WordCountLocal {
    public static void main(String[] args) throws Exception {

//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://master:9000");
        // 创建一个job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountLocal.class);

        // 设置Job对应参数：Map 输入输出参数
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应参数： Reduce输入输出参数
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Job对应参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("input/wc.txt"));
        FileOutputFormat.setOutputPath(job, new Path("input/out"));

        // 提交job作业
        boolean result = job.waitForCompletion(true);

        // 提交是否成功
        System.exit(result ? 0 : -1);
    }
}
