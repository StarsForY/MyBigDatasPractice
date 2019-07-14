package com.chinasofti.mapreducepractice.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/*
 * 自定义的数据封装类，用于封装需求数据
 * Java 的序列化（Serializable）是一个重量级序列化框架，
 * 一个对象被序列化后，会附带很多额外的信息（各种校验信息，header，继承体系…），不便于在网络中高效传输；
 * hadoop 自己开发了一套序列化机制（**Writable**），精简，高效。
 * 不用像 java 对象类一样传输多层的父子关系，需要哪个属性就传输哪个属性值，大大的减少网络传输的开销。
 * 数据封装类需要实现序列化接口，Hadoop有自己的序列化接口Writable，实现Hadoop的Writable接口
 */
public class FlowBean implements Writable {

    // 我们要保存三个数据，所以在类中定义三个参数
    private long upflow = 0;    // 上行流量
    private long downflow = 0;  // 下行流量
    private long sumflow = 0;   // 总流量

    // 创建无参构造
    // 注意：如果没有创建这个无参构造方法，再反序列化的时候会出错
    public FlowBean(){
        super();
    }

    // 创建有参构造
    public FlowBean(long upflow, long downflow, long sumflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = sumflow;
    }

    //因为入参的时候有时会入下行流量和上行流量，没有入总流量，所有需要再建立一个这样的有参构造方法
    public FlowBean(long upflow, long downflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow+downflow;
    }
    // bean对象常规操作：为属性提供get和set方法
    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    // set数据时没有set方法，所以我们在这里手动编写一个set方法
    // 主要实现：给属性复赋值
    public void set(long upflow, long downflow){
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow+downflow;
    }

    //这就序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 先序列化上行流量，再序列化下行流量，最后序列化总流量
        // 序列化的方法在 DataOutput 类中以write开头的方法
        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    //这是反序列化方法
    //反序列时候  注意序列化的顺序
    //先序列化的先出来
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 注意反序列化的顺序，先反序列化上行流量，再反序列化下行流量，最后反序列化总流量
        // 反序列化的方法在 DataInput 类中以read开头的方法
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();
    }

    // 重写toSting方法
    @Override
    public String toString() {
        return upflow+"\t"+downflow+"\t"+sumflow;
    }
}
