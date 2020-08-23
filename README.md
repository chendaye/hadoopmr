# 环境

- Hadoop 2.6.0-cdh5.15.2 http://localhost:50070/  http://localhost:9000/
- Hive 1.1.0-cdh5.15.2
- Yarn http://localhost:8088/
- Java 1.8

# 提交任务

> http://localhost:8088/ 在 Yarn 上查看运行情况

```bash
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0-cdh5.15.2.jar pi
```

# 项目背景

> 电商行为日志分析: 用户访问搜索会产生日志
>日志包括字段：来源（refer）、所用浏览器、访问IP、访问时间、访问页面

# 项目需求

- 统计页面浏览量
- 统计各个省份的浏览量
- 统计页面的访问量最多的前几个省（TOPN）

# 项目架构

> 日志文件 导入HDFS，按天分区
> 实现方式有：MR、Hive
> 处理完导入 HDFS 再从HDFS导入Mysql

# 项目问题

## 数据量大

> 数据量很大，处理很慢

> 解决方式：ETL（数据清洗转换）、数据压缩

- 去除无用的字段
- 解析IP
- 清洗后的数据再存入HDFS，后面再基于清洗的数据进行分析

## job 超时失败

> 一旦 application master 注意到已经有一段时间没有收到进度的更新，便会将任务标记为失败。
> 在此之后，任务 JVM 进程将被自动杀死
> mapreduce.task.timeout ，单位为毫秒 （通常10min）