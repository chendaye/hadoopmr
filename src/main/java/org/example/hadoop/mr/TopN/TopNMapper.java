package org.example.hadoop.mr.TopN;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  求topn
 */
public class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
    private TreeMap<Integer, Text> treemap = new TreeMap<Integer, Text>();	//TreeMap是有序KV集合

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            return;
        }
        String[] strs = value.toString().split(" "); // t001 2067
        String tId = strs[0];
        String num = strs[1];
        if (tId == null || num == null) {
            return;
        }
        // 2067 -> "t001 2067" 大小为 n的堆
        treemap.put(Integer.parseInt(num), new Text(value));	//将访问次数（KEY）和行记录（VALUE）放入TreeMap中自动排序
        if (treemap.size() > 10) {	//如果TreeMap中元素超过N个，将第一个（KEY最小的）元素删除
            treemap.remove(treemap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text t : treemap.values()) {
            context.write(NullWritable.get(), t);	//todo：在clean()中完成Map输出
        }
    }
}
