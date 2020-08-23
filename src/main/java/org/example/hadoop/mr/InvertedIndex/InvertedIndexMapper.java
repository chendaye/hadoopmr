package org.example.hadoop.mr.InvertedIndex;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();
    private FileSplit split;
    private int line = 0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        String[] s = value.toString().split(" ");
        for (String str : s){
            context.write(new Text(str+":"+line), new Text("1"));
        }
        line++;
    }
}
