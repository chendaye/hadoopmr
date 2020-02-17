package org.example.hadoop.mr.train;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper 类
 */
public class ClickMapper extends Mapper<LongWritable, Text, Text, Click> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分每一行
        String[] items = value.toString().split("\t");
        // 取IP click
        String ip = items[3];
        long click = Long.parseLong(items[items.length - 2]);

        // 写Context
        context.write(new Text(ip), new Click(ip, click));
    }
}
