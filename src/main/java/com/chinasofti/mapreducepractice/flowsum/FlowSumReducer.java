package com.chinasofti.mapreducepractice.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 这里是MR程序 reducer阶段处理的类
 * KEYIN：就是reducer阶段输入的数据key类型，对应mapper的输出key类型  手机号
 * VALUEIN就是reducer阶段输入的数据value类型，对应mapper的输出value类型  自己封装的bean对象
 * KEYOUT就是reducer阶段输出的数据key类型 在本案例中  手机号
 * VALUEOUTreducer阶段输出的数据value类型 在本案例中  求和后的总流量
        */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 为了统计求和，获取手机流量的总和，这里设置两个参数用来保存值
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        
        // reduce做的事情很简单，只需要把流量拿出来统计即可
        for (FlowBean flowBean:values) {
            sumUpFlow += flowBean.getUpflow();
            sumDownFlow += flowBean.getDownflow();
        }
        // key还是之前的手机号，所以不用变，
        // value是计算后的值，这里同样为了避免创建多个对象而造成垃圾，先创建好对象后再来set值
        v.set(sumUpFlow,sumDownFlow);

        context.write(key,v);
    }
}
