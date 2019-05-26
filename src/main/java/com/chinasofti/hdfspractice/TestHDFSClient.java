package com.chinasofti.hdfspractice;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;

public class TestHDFSClient {
    public static void main(String[] args) throws Exception {

        Configuration conf =  new Configuration();
        // 这里指定使用的是 hdfs 文件系统
        conf.set("fs.defaultFS","hdfs://node-1:9000");
        // 通过如下的方式进行客户端身份的设置，如果没有设置，将会以window上的用户去执行下面你的代码，
        // 但是下面的命令只有root用户有权限执行，之前我们并没有赋权给到当前用户。
        // 除了这个方法能够使用root用户去执行命令外，也可以通过如下的方式去指定文件系统的类型 并且同时设置用户身份
        //FileSystem fs = FileSystem.get(new URI("hdfs://node-21:9000"), conf, "root");
        System.setProperty("HADOOP_USER_NAME", "root");

        // 获取文件系统对象,需要Configuration类对象的配置
        FileSystem fs = FileSystem.get(conf);
        // 创建一个目录，create方法需要一个Path对象，我们直接new一个对象给到它，
        // 同时Path对象需要一个字符串类型的参数，这个参数指定路径。
         fs.create(new Path("/hellobyjava"));

        // 从hdfs上下载一个文件到本地
        // 这个方法有两个参数，第一个是一个Path对象，传入的是下载位置，
        // 第二个参数也是一个path对象，传入的是下载后保存的位置
        fs.copyToLocalFile(false,new Path("/hellohdfs/1.txt"),new Path("D:\\Practice_File\\java\\bigdata-practice\\src\\main"),true);

        //使用Stream的形式 操作HDFS 更底层的方式
        //创建一个输出流，表示将文件输出到哪个位置，这里的路径是：/hellohdfs/2.txt
        FSDataOutputStream outputStream = fs.create(new Path("/hellohdfs/2.txt"), true);
        // 创建一个输入流，表示文件是从哪里输入的，这里是从D:\Practice_File\java\bigdata-practice\src\main
        FileInputStream inputStream = new FileInputStream("D:\\Practice_File\\java\\bigdata-practice\\src\\main\\2.txt");
        // 调用 org.apache.commons.io.IOUtils 包下的类实现流传递
        IOUtils.copy(inputStream, outputStream);

        // 使用完记得关闭
        fs.close();

    }
}
