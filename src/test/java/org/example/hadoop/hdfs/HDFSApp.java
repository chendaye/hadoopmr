package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSApp {
    private FileSystem fileSystem = null;
    private Configuration configuration = null;
    private String path = "hdfs://master:9000";

    /**
     * 单元测试前要执行的方法
     * */
    @Before
    public void setUp() throws Exception{
        configuration = new Configuration();
        // 设置副本数=1
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(path), configuration, "hadoop");
    }

    /**
     * 测试方法
     * 创建HDFS文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("lengo"));
    }

    /**
     * 从HDFS读取文件
     * @throws Exception
     */
    @Test
    public void text() throws Exception{
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/hadoop/lengo/.profile"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    /**
     * 向HDFS写文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/hadoop/lengo/write"));
        fsDataOutputStream.writeUTF("Hello lengo");
        fsDataOutputStream.flush(); // 情况缓冲区
        fsDataOutputStream.close();
    }

    /**
     * 测试修改文件名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldN = new Path("/user/hadoop/lengo/.profile");
        Path newN = new Path("/user/hadoop/lengo/.profile_new");
        fileSystem.rename(oldN, newN);
    }

    /**
     * 上传本地文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyfile() throws Exception{
        Path src = new Path("E:\\下载\\chorme\\Pycharm.png"); // 本地文件
        Path dst = new Path("/user/hadoop/lengo/"); // 上传地址
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 上传大文件
     * 带进度条
     * @throws Exception
     */
    @Test
    public void copyBigFile() throws Exception{
        // 读取本地文件（流）
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\下载\\chorme\\sbt-1.3.6.msi")));
        // 上传流文件
        FSDataOutputStream out = fileSystem.create(new Path("/user/hadoop/lengo/sbt.mis"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("=");
                    }
                });
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 下载HDFS上的文件到本地
     * @throws Exception
     */
    @Test
    public void copyToLocal() throws Exception{
        Path src = new Path("/user/hadoop/lengo/write");  // 资源地址
        Path dst = new Path("E:\\下载\\chorme"); // 本地地址
        fileSystem.copyToLocalFile(src, dst);
    }

    /**
     * HDFS 上的问件列表
     * @throws Exception
     */
    @Test
    public void listFiles()throws Exception{
        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus file : statuses){
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            long length = file.getLen();
            short reolication = file.getReplication();
            String path = file.getPath().toString();

            System.out.println(
                    isDir + "\t" + permission + "\t" + reolication + "\t" + length + "\t" + path
            );

        }
    }

    /**
     * 递归的列出文件夹下的所有内容
     * @throws Exception
     */
    @Test
    public void listsFilesRecursive() throws Exception{
        // 善于查看api
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/user"), true);
        while (remoteIterator.hasNext()){
            LocatedFileStatus file = remoteIterator.next(); // 迭代
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            long length = file.getLen();
            short reolication = file.getReplication();
            String path = file.getPath().toString();

            System.out.println(
                    isDir + "\t" + permission + "\t" + reolication + "\t" + length + "\t" + path
            );
        }
    }

    /**
     * 获取文件的块信息
     * @throws Exception
     */
    @Test
    public void getBlock() throws Exception{
        // 文件句柄
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/csapp.pdf"));
        // 获取文件块的位置
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation blockLocation : blockLocations){
            for (String name: blockLocation.getNames()){
                System.out.println(name + ":" + blockLocation.getOffset() + ":" + blockLocation.getLength());
            }
        }
    }

    /**
     * 删除hdfs文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/user/hadoop/lengo/write"), false);
    }

    /**
     * 单元测试之后执行的方法
     * 释放资源
     */
    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
    }
}
