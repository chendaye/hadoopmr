package org.example.hadoop.mr.Join.ReducerJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.hadoop.mr.Sort.Sort;
import org.example.hadoop.mr.Sort.SortComparator;

import java.io.File;
import java.io.IOException;

/**
 * todo: Reducer 端的Join
 *
 * Map端主要工作:
 * 为来自不同表或文件的k-v键值对，打标签以区别不同的来源，以连接字段作为key，其余部分加上标签作为value，然后输出.
 *
 * Reduce端主要工作
 * 在Reduce端以连接字段作为key的分组已经完成，只需要在每一个分组中，把来源自不同表/文件的记录通过标签分开，最后合并.
 */
public class ReducerJoin {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        job.setJarByClass(ReducerJoin.class);

        //map
        job.setMapperClass(ReducerJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ReducerJoinWritable.class);

        // reducer
        job.setReducerClass(ReducerJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 输入输出
        FileInputFormat.setInputPaths(job,new Path("input/join"));
        FileOutputFormat.setOutputPath(job,new Path("out/join"));
        File file = new File("out/join");
        if(file.exists()){
            file.delete();
        }
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "成功" : "失败");
    }
}
