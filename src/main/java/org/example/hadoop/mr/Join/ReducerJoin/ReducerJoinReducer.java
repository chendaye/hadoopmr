package org.example.hadoop.mr.Join.ReducerJoin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReducerJoinReducer extends Reducer<IntWritable, ReducerJoinWritable, Text, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<ReducerJoinWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<ReducerJoinWritable> list = new ArrayList<>();
        ReducerJoinWritable pt = null;
        for (ReducerJoinWritable val : values){
            if (val.getFlag().equals("product")){
                pt = val; // 公司
            }else {
                list.add(val); // 订单
            }
        }
        for (ReducerJoinWritable val : list){
            String res = "";
            System.out.println(val.getAmount());

            if (pt != null)
                res = pt.getCompany()+" "+val.getPid()+" "+val.getAmount();
            else
                res = "-"+" "+val.getPid()+" "+val.getAmount();
            context.write(new Text(res), NullWritable.get());
        }
    }
}
