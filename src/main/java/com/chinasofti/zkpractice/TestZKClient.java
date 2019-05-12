package com.chinasofti.zkpractice;

import org.apache.zookeeper.*;

public class TestZKClient {
    public static void main(String[] args) throws Exception { // 这里抛出一个最大异常（练习可以，工作中不允许）
        // 初始化 ZooKeeper 实例(zk 地址、会话超时时间，与系统默认一致、watcher)
        // 这里两个zk地址：程序尝试连接第一个地址，当连接不上时，会去连接第二个
        // 会话超时时间以毫秒计，这里是3秒
        // 最后一个是一个watcher 监听类，需要抛出一个IO异常，这里直接new了一个内部类来完成参数的传递
        ZooKeeper zk = new ZooKeeper("node-1:2181,node-2:2181", 30000, new Watcher() {
            // 这里是事件通知的回调方法
            public void process(WatchedEvent event) {
                System.out.println("事件类型为：" + event.getType());         // 打印事件的通知类型
                System.out.println("事件发生的路径：" + event.getPath());     // 打印事件的通知发生的路径
                System.out.println("通知状态为：" +event.getState());         // 打印事件的通知状态
            }
        });

        // 创建一个持久的序列化节点
        // 第一个参数指定的是节点创建的位置，参数类型是String类型
        // 第二个节点是保存的数据，参数类型是 Bytes[]
        // 第三个参数是权限限制，这个了解的不多  T.T  谅解谅解，怕理解错了，带偏了
        // 第四个为创建节点的类型，当你输入Create之后IDEA就会有提示
        // 这里是创建了一个序列化持久节点
        zk.create("/MyGirls","可爱的".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);

        zk.close();
    }
}
