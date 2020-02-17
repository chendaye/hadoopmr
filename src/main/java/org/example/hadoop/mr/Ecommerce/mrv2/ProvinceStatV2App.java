package org.example.hadoop.mr.Ecommerce.mrv2;

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
 * 统计省份的浏览量
 */
public class ProvinceStatV2App {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProvinceStatV2App.class);
        job.setMapperClass(ProvinceStatV2App.MyMapper.class);
        job.setReducerClass(ProvinceStatV2App.MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/epl/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/v2/ProvinceStat"));

        job.waitForCompletion(true);
    }


    /**
     * 静态内部类
     */
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString(); // 拿到日志

            Map<String,String> info = logParser.parserV2(log);
            String province = info.get("province");

            if(StringUtils.isNotBlank(province)){
                context.write(new Text(province), ONE);
            }else {
                context.write(new Text("-"), ONE);
            }
        }
    }


    static class MyReducer extends Reducer<Text, LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for (LongWritable value: values){
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}
