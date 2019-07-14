package com.chinasofti.mapreducepractice.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSumDriver {
    public static void main(String[] args) throws Exception {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(FlowSumDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 暂不用设置reduce数量
//        job.setNumReduceTasks(3);
        //如果业务有需求，就可以设置combiner组件
//        job.setCombinerClass(FlowSumReducer.class);

        /*
         * 本地运行语句
         * 注意本地运行时输出目录也不能存在，否则也会报错
         */
        FileInputFormat.setInputPaths(job,new Path("D:\\Practice_File\\hadoop_practice\\MapReduce\\flowsum\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\Practice_File\\hadoop_practice\\MapReduce\\flowsum\\output"));
//        job.submit();
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
