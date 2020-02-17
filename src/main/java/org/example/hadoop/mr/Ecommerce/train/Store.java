package org.example.hadoop.mr.Ecommerce.train;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.hadoop.mr.Ecommerce.utils.LogParser;

import java.io.IOException;
import java.util.Map;

/**
 * 统计每个店的记录数
 */
public class Store {
    /**
     * 主方法
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 设置job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置主类
        job.setJarByClass(Store.class);

        // Mapper
        job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer
        job.setReducerClass(StoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 输入输出
        FileInputFormat.setInputPaths(job, new Path("input/epl/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/train/store"));

        job.waitForCompletion(true);
    }

    /**
     * Mapper
     */
    static class StoreMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        private LogParser logParser = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> log = logParser.parserV2(value.toString());
            String store = log.get("store");
            if (StringUtils.isNotBlank(store)){
                context.write(new Text(store), new LongWritable(1));
            }else{
                context.write(new Text("-"), new LongWritable(1));
            }
        }
    }

    /**
     * Reducer
     */
    static class StoreReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for (LongWritable val: values){
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}
