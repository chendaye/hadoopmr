package org.example.hadoop.mr.Distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 数据去重： 去掉重复日期的数据
 *
 * TODO: 思路
 *      - 只要搞清楚了MR的流程这个就很简单，reducer的输入类似<key3,[v1,v2,v3...]>，这个地方输入的key3是没有重复值的。
 *      - 所以利用这一点，Mapper输出的key保存日期数据，value置为空即可 【这里可以使用NullWritable类型】
 */
public class Distinct {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 入口
        job.setJarByClass(Distinct.class);

        // map
        job.setMapperClass(DistinctMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // reduce
        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 输入输出
        FileInputFormat.setInputPaths(job, new Path("input/date.txt"));
        FileOutputFormat.setOutputPath(job, new Path("input/out/distinct"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "成功" : "失败");
    }
}
