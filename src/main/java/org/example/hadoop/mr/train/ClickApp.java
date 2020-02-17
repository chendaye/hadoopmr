package org.example.hadoop.mr.train;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;

public class ClickApp {
    public static void main(String[] args) throws Exception {
        // 配置job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ClickApp.class);

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
        FileInputFormat.setInputPaths(job, new Path("input/data/access.log"));
        FileOutputFormat.setOutputPath(job,new Path("input/ouput"));

        // 运行
        job.waitForCompletion(true);
    }
}
