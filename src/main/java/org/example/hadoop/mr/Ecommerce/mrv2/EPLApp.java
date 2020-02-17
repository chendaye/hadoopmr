package org.example.hadoop.mr.Ecommerce.mrv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.hadoop.mr.Ecommerce.utils.LogParser;

import java.io.IOException;
import java.util.Map;

/**
 * 统计省份的浏览量
 */
public class EPLApp {
    public static void main(String[] args) throws Exception {
        // 配置job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置主类
        job.setJarByClass(EPLApp.class);

        // 设置map;不需要reducer，把每一行进行清洗
        job.setMapperClass(EPLApp.MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 输入输出
        FileInputFormat.setInputPaths(job, new Path("input/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, new Path("output/epl"));

        job.waitForCompletion(true);
    }


    /**
     * 静态内部类
     */
    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString(); // 拿到日志

            Map<String,String> info = logParser.parser(log);

            String ip = info.get("ip");
            String store = info.get("store");
            String browser = info.get("browser");
            String system = info.get("system");
            String country = info.get("country");
            String province = info.get("province");
            String city = info.get("city");
            String time = info.get("time");
            String url = info.get("url");
            String pageId = info.get("pageId");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ip).append("\t");
            stringBuilder.append(store).append("\t");
            stringBuilder.append(browser).append("\t");
            stringBuilder.append(system).append("\t");
            stringBuilder.append(country).append("\t");
            stringBuilder.append(province).append("\t");
            stringBuilder.append(city).append("\t");
            stringBuilder.append(time).append("\t");
            stringBuilder.append(url).append("\t");
            stringBuilder.append(pageId).append("\t");

            // 把处理过的数据存起来
            context.write(NullWritable.get(), new Text(stringBuilder.toString()));
        }
    }
}
