package org.example.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce 自定义分区规则
 */
public class AccessPartitioner extends Partitioner<Text, Access> {
    /**
     *
     * @param phone 手机号
     * @param access 统计信息
     * @param numReduceTasks reducer 任务数
     * @return
     */
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {
        if(phone.toString().startsWith("13")){
            return 0;   // 13开头的手机号到 0号分区
        }else if(phone.toString().startsWith("15")){
            return 1;   // 15开头的手机号到 1号分区
        }else {
            return 2;   // 其他的到 2号分区
        }
    }
}
