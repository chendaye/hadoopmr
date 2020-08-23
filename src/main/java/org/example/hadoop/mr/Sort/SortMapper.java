package org.example.hadoop.mr.Sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable num = new IntWritable();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        num.set(Integer.parseInt(s[1]));
        context.write(num, new Text(s[0])); // 1 2017-02-14 ： 按 1 倒序排列
    }
}
