package org.example.hadoop.mr.train;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class ClickReducer extends Reducer<Text, Click, Text, Click> {
    @Override
    protected void reduce(Text key, Iterable<Click> values, Context context) throws IOException, InterruptedException {
        String ip = "";
        long clicks = 0;

        for(Click value: values){
            ip = value.getIp();
            clicks += value.getClick();
        }

        context.write(key, new Click(ip, clicks));
    }
}
