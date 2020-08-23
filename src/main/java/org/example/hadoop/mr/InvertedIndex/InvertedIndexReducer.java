package org.example.hadoop.mr.InvertedIndex;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    /**
     * <单词， <行号:数量, 行号:数量, 行号:数量>>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + "; ";
        }
        result.set(fileList);
        context.write(key, result);		//输出格式："word	file1:num1; file2:num2;"
    }
}
