package org.example.hadoop.mr.train;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 根据ip开头划分分区
 */
public class ClickPartitioner extends Partitioner<Text, ClickWritable> {
    @Override
    public int getPartition(Text ip, ClickWritable click, int numPartitions) {
        if(ip.toString().startsWith("120.197")){
            return 0;
        }else if (ip.toString().startsWith("120.196")){
            return 1;
        }else {
            return 2;
        }
    }
}
