package org.example.hadoop.mr.TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * 就是每一个Map Task任务要求其只输出TopN数据，这里借助TreeMap自动排序的特性【 将数字作为排序键 】，保证TopN。
 * 然后是Reduce中再次求解TopN即可
 *
 * todo: 对于每一个maper 任务，按 key 排序，并维持一个 大小为 n 的堆
 *         - 每一个 mapper 任务最后都生成一个堆， 堆得内容写到 mapper 结果文件中
 *
 * todo: 设置 Reducer 任务个数为 1 ， 最后，所有mapper 生成的局部的堆，全汇聚到 reducer
 *      - reducer 维持一个堆 大小为 n，得到最后结果
 */
public class TopN {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(TopN.class);

        // map
        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reducer
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);		//计算最终TopN，只能运行一个Reduce任务

        FileInputFormat.setInputPaths(job, new Path("input/topn.txt"));
        FileOutputFormat.setOutputPath(job, new Path("out/topn"));
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(new Path("out/topn"))) {
            fs.delete(new Path("out/topn"), true);
        }

        if(job.waitForCompletion(true)) {
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }
    }
}
