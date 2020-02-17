package org.example.hadoop.mr.Ecommerce.mrv1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 第一版本 统计页面浏览量
 */
public class PVStatApp {
    public static void main(String[] args)  throws Exception {
        // 配置
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // main class
        job.setJarByClass(PVStatApp.class);

        // Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // 输入输出
        FileInputFormat.setInputPaths(job, new Path("input/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, new Path("output/v1/PVStat"));

        // 提交
        job.waitForCompletion(true);
    }

    /**
     * 静态内部类
     *
     * Mapper
     *
     * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{

        // 输入的key value
        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY, ONE);
        }
    }

    /**
     * 静态内部类
     *
     * Reducer
     *
     * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for (LongWritable value : values){
                count++;
            }

            context.write(NullWritable.get(), new LongWritable(count));
        }
    }
}
