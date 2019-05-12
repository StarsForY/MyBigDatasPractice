package com.chinasofti.zkpractice;

/*
    这部分是一些常见的API操作，避免篇幅过大，所以没有写这些练习
*/

import org.apache.zookeeper.*;

public class ZKOthersAPI {
    public static void main(String[] args) throws Exception {
        // 初始化 ZooKeeper 实例(zk 地址、会话超时时间，与系统默认一致、watcher)
        ZooKeeper zk = new ZooKeeper("node-21:2181,node-22:2181", 30000, new Watcher() {

            public void process(WatchedEvent event) {
                System.out.println("事件类型为：" + event.getType());
                System.out.println("事件发生的路径：" + event.getPath());
                System.out.println("通知状态为：" +event.getState());
            }
        });
        // 创建一个目录节点
        zk.create("/testRootPath", "testRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // 创建一个子目录节点
        zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println(new String(zk.getData("/testRootPath",false,null)));

        // 取出子目录节点列表
        System.out.println(zk.getChildren("/testRootPath",true));

        // 修改子目录节点数据
        zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1);
        System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]");

        // 创建另外一个子目录节点
        zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println(new
                String(zk.getData("/testRootPath/testChildPathTwo",true,null)));

        // 删除子目录节点
        zk.delete("/testRootPath/testChildPathTwo",-1);
        zk.delete("/testRootPath/testChildPathOne",-1);

        // 删除父目录节点
        zk.delete("/testRootPath",-1);
        zk.close();
    }
}
