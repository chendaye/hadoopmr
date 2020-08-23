package org.example.hadoop.mr.Sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * todo: 自定义 日期比较器
 */
public class SortComparator extends WritableComparator {
    public SortComparator(){
        super(IntWritable.class, true);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" }) // 告诉编译器忽略指定的警告，不用在编译完成后出现警告信息
    public int compare(WritableComparable a, WritableComparable b) {
        // CompareTo方法，返回值为1则降序，-1则升序
        // 默认是a.compareTo(b)，a比b小返回-1，现在反过来返回1，就变成了降序
        return b.compareTo(a);
    }
}
