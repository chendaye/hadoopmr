package org.example.hadoop.mr.Distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Object, Text 输入
 * Text, NullWritable 输出
 */
public class DistinctMapper extends Mapper<Object, Text, Text, NullWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" "); // 2017-02-14 1
        // 用date 作为 key
        context.write(new Text(s[0]), NullWritable.get());
    }


}
