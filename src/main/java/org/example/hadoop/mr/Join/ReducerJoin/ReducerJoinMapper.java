package org.example.hadoop.mr.Join.ReducerJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;


public class ReducerJoinMapper extends Mapper<LongWritable, Text, IntWritable, ReducerJoinWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        String filename = split.getPath().getName();
        String[] fields = value.toString().split(" ");
        ReducerJoinWritable writable = new ReducerJoinWritable();
        IntWritable k = new IntWritable();
        if (filename.contains("order")){
            writable.setPid(Integer.parseInt(fields[1]));
            writable.setAmount(Integer.parseInt(fields[2]));
            writable.setCompany("-");
            writable.setFlag("order");
            k.set(Integer.parseInt(fields[1]));
        }else{
            writable.setPid(Integer.parseInt(fields[0]));
            writable.setAmount(0);
            writable.setCompany(fields[1]);
            writable.setFlag("product");
            k.set(Integer.parseInt(fields[0]));
        }
        context.write(k, writable);
    }
}
