自定义复杂类型

access.log

  第二个字段： 手机号
  
  倒数第三个字段： 上行流量
  倒数第二个字段： 下行流量
  
  需求：统计每一个手机号的：上行流量和 下行流量和 总的流量和
  
  复杂数据类型：
    Access.java
     包含:
     - 手机号
     - 上行流量
     - 下行流量
     -  总流量
     
     
  根据手机号分组，把对应的上下行流量加起来
  
  
  Mapper: 手机号  上行流量  下行流量  拆开
  把手机号作为 key ； Access 作为 value  写出去
  
  
  Reducer: (15271834241, <Access, Access>)
  
 ```java
public class HashPartitioner<K, V> extends Partitioner<K, V> {
    public HashPartitioner() {
    }

    public int getPartition(K key, V value, int numReduceTasks) {
        return (key.hashCode() & 2147483647) % numReduceTasks;
    }
}
```

HashPartitioner： Hadoop默认的分区规则

numReduceTasks: 作业指定的reducer个数， 决定了reducer作业输出文件的个数

reducer 个数 3

1%3 = 1
2%3 = 2
3%3 = 0

取模决定了，map的输出 放在哪个 reducer上面处理


需求：将结果按手机号的前缀输出到不同的文件

13* ===》 。。
15* ===》 。。
other ===》 。。

需要3个reducer

Partitioner 决定mapTask 输出的数据交由哪一个 reduceTask 处理

默认实现：分发的key的hash值 与 reduce 的个数 取模

    