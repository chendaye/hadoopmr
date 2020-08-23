package org.example.hadoop.mr.InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.hadoop.mr.TopN.TopN;

import java.io.IOException;

/**
 * todo: 倒排索引
 *      - 什么是倒排索引？
 *          - 为了快速在文档中找到 某一个单词在哪
 *              - A: 第一页 第三页
 *              - B: 第一页 第二页
 *              - C: 第一页 第三页
 *              - D: 第二页
 *      - 正排索引？
 *              - 第一页：A B C
 *              - 第二页：B C D
 *              - 第三页： A C
 */
public class InvertedIndex {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(InvertedIndex.class);

        //map
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(InvertedIndexCombiner.class); // 设置 map端的 combiner

        //reducer
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入输出
        FileInputFormat.setInputPaths(job,new Path("input/index.txt"));
        FileOutputFormat.setOutputPath(job,new Path("out/index/date.txt"));


        if(job.waitForCompletion(true)) {
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }
    }
}
