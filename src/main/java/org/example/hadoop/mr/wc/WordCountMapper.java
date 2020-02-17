package org.example.hadoop.mr.wc;

// 注意导入的包

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 输入是一行字符串 key是 这一行的偏移量，value是字符 (100, long,lengo,long)
 * KEYIN:  Map任务读数据的key类型，是每一行数据起始位置的偏移量 long
 * VALUEIN：Map任务读数据的value 类型，就是一行行字符串 String
 *
 *
 *
 * 输出的键是单词，输出的key是 单词，  value是计数  (long, 1） (long, 1）(lengo, 1）
 * KEYOUT: map方法自定义实现输出的key的类型 String
 * VALUEOUT: map 方法自定义实现输出的value的类型 Integer
 *
 * Long String String Integer 是java原生的数据类型
 *
 * Hadoop有自定义的类型：支持 序列化和反序列化。   LongWritable Text
 *
 * Reduce 和 Mapper 中使用到了设计模式： 模板
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(",");

        for (String word: words){
            // 把内容写入 context (hello,1) (world,1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
