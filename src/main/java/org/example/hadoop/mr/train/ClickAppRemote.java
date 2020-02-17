package org.example.hadoop.mr.train;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClickAppRemote {
    public static void main(String[] args) throws Exception {
        // 配置job
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ClickAppRemote.class);

        // 设置Mapper
        job.setMapperClass(ClickMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Click.class);

        // 设置Reducer
        job.setReducerClass(ClickReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Click.class);

        // 设置分区
        job.setPartitionerClass(ClickPartitioner.class);
        job.setNumReduceTasks(3);

        // 输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/user/hadoop/access.log"));
        FileOutputFormat.setOutputPath(job,new Path("/user/hadoop/access-out"));

        // 运行
        job.waitForCompletion(true);
    }
}
