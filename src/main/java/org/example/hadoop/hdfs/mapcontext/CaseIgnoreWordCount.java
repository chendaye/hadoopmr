package org.example.hadoop.hdfs.mapcontext;

public class CaseIgnoreWordCount implements WCMapper {
    @Override
    public void map(String line, WCContext context) {
        // 空格 分割
        String[] words = line.toLowerCase().split(",");
        // 遍历每一个单词
        for (String word: words){
            // context 中是否已经有 word
            Object value = context.get(word);
            if (value == null){
                // 没有就新增，词频=1
                context.write(word, 1);
            } else {
                // 获取词频
                int count = Integer.parseInt(value.toString());
                // 词频+1
                context.write(word, count + 1);
            }
        }
    }
}
