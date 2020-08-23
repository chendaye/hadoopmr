package org.example.hadoop.mr.Sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR 排序： 这一类问题很多，像学生按成绩排序，手机用户流量按上行流量升序，下行流量降序排序等等
 *
 * TODO： 思路
 *      MapReduce是默认会对key进行升序排序的，可以利用这一点实现某些排序
 *      单列排序
 *          升序还是降序排序
 *          可以利用Shuffle默认对key排序的规则；
 *          自定义继承WritableComparator的排序类，实现compare方法
 *       二次排序
 *          实现可序列化的比较类WritableComparable<T>，并实现compareTo方法（同样可指定升序降序）
 *          日期按计数升序排序
 */
public class Sort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 自定义比较器
        job.setSortComparatorClass(SortComparator.class);

        job.setJarByClass(Sort.class);
        // map
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reducer
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入输出
        FileInputFormat.setInputPaths(job,new Path("input/date.txt"));
        FileOutputFormat.setOutputPath(job,new Path("out/sort/date.txt"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "成功" : "失败");
    }
}
