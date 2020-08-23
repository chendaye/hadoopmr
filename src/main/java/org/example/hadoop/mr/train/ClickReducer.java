package org.example.hadoop.mr.train;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class ClickReducer extends Reducer<Text, ClickWritable, Text, ClickWritable> {
    @Override
    protected void reduce(Text key, Iterable<ClickWritable> values, Context context) throws IOException, InterruptedException {
        String ip = "";
        long clicks = 0;

        for(ClickWritable value: values){
            ip = value.getIp();
            clicks += value.getClick();
        }

        context.write(key, new ClickWritable(ip, clicks));
    }
}
