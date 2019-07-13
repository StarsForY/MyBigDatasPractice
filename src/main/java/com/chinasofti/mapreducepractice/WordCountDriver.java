package com.chinasofti.mapreducepractice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时候所需要的信息
 * 比如：使用的是那个Mapper类  那个Reducer类  输入数据在那 输出数据在什么地方
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        // 当需要运行本地模式时启动下面这句话，当然，这句话不是必输的，
        // 当没有输入这句话的时候，Hadoop会读取默认的配置文件，并且默认配置是在本地运行。
        conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(3);
        //如果业务有需求，就可以设置combiner组件
        job.setCombinerClass(WordCountReducer.class);


        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        // 注意输出文件夹不能存在，如果存在则会报错，这是Hadoop避免出现数据覆盖的现象
        // 其实想一想就明白了，如果自己辛辛苦苦跑了半天的数据一不小心被另一个程序覆盖了那也挺奔溃的。
        /*
         * 集群运行语句
         */
//        FileInputFormat.setInputPaths(job,"/wordcount/input");
//        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));
        /*
         * 本地运行语句
         */
        FileInputFormat.setInputPaths(job,"D:\\Practice_File\\hadoop_practice\\MapReduce\\input");
        FileOutputFormat.setOutputPath(job,new Path("D:\\Practice_File\\hadoop_practice\\MapReduce\\output"));
//        job.submit();
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
