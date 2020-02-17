package org.example.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Mapper
 * 第一个参数： 文本的偏移量
 * 第二个参数： 文本
 * 第三个参数： key （电话号码）
 * 第四个参数：value （自定义的复杂类型Access）
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    /**
     *
     * @param key 电话号码
     * @param value 文本的一行
     * @param context 保存内容的容器
     * @throws IOException  IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆开一行
        String[] lines = value.toString().split("\t");
        String phone = lines[1]; // 电话
        Long up = Long.parseLong(lines[lines.length - 3]); // 上行流量
        Long down = Long.parseLong(lines[lines.length - 2]); // 上行流量

        // 写
        context.write(new Text(phone), new Access(phone, up, down));
    }
}
