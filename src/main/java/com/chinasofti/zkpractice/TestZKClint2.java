package com.chinasofti.zkpractice;

import org.apache.zookeeper.*;

public class TestZKClint2 {
    public static void main(String[] args) throws Exception {   // 这里抛出一个最大异常（练习可以，工作中不允许）
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

        // 因为之前创建了节点，所以这里就不再创建节点了，直接测试

        // 相对于节点：MyGirls 设置了数据变化的监听，一旦数据改变，就会触发监听
        // 第一个参数：表示你要获取那个路径下的节点
        // 第二个参数：表示是否要开启监听，true表示是，false表示否
        // 第三个参数：节点的状态
        zk.getData("/MyGirls0000000003",true,null);

        // 修改节点数据，因为上面设置了监听，所以这里修改会触发监听。
        // 第一个参数：表示你要修改哪个路径下的节点
        // 第二个参数：修改的内容，数据格式为data[]
        // 第三个参数：对应的版本号，-1 表示版本号不用手工维护，由系统来维护
        zk.setData("/MyGirls0000000003","温柔的".getBytes(),-1);

        zk.close();
    }
}
