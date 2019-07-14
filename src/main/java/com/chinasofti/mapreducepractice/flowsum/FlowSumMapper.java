package com.chinasofti.mapreducepractice.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * 这里就是mapreduce程序  mapper阶段业务逻辑实现的类
 * KEYIN：KEYIN就表示每一行的起始偏移量  因此数据类型是Long
 * VALUEIN:VALUEIN就表示读取的这一行内容  因此数据类型是String
 * KEYOUT：KEYOUT表示手机号，因此数据类型是String
 * VALUEOUT：VALUEOUT表示输出的封装好的数据，因此数据类型是 封装好的bean对象
 */

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    // mapper类输出的数据是两个对象，不能每运行一次map方法就创建两个对象，这势必会造成很多垃圾
    // 我们可以先创建这两个对象，每次运行map方法时将值set这两个对象，从而避免创建多个对象的问题
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每一行的内容
        String line = value.toString();
        //将数据按 "/t" 进行切分，这个切分方式是根据数据源来决定的
        String[] fields = line.split("\t");
        //切分后即可获取手机号和上下行流量
        // 手机号就是第二个
        String phoneNum = fields[1];
        // 上行流量是倒数第三个，切分完后数据类型是String，需要转换成Long
        long upflow = Long.parseLong(fields[fields.length - 3]);
        // 下行流量是倒数第二个，切分完后数据类型是String，需要转换成Long
        long downflow = Long.parseLong(fields[fields.length - 2]);

        // 为了避免重复创建对象而产生垃圾，我们先创建好key，value对象，然后通过各自的set方法去赋值
        k.set(phoneNum);
        // 因为数据封装类中没有set方法，所以我们自己去类中编写一个set方法
        v.set(upflow,downflow);

        // 通过context对象将数据传递给reduce
        context.write(k,v);

    }
}
