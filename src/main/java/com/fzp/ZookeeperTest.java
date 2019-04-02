package com.fzp;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperTest {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        // 创建Watcher事件通知机制
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                watchedEvent.getType();
            }
        };

        // 创建Zookeeper连接
        ZooKeeper zk = new ZooKeeper("node02:2181",3000,watcher);

        // 创建节点
//        String result = zk.create("/zkTest", "dataxxx".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println("Process:"+result);
        // 获取节点
//        byte[] data = zk.getData("/zkTest", false, null);
//        String dataStr = new String(data);
//        System.out.println("Process:"+dataStr);
        // 删除节点
//        zk.delete("/zkTest",-1);
//        Stat stat = zk.exists("/zkTest",watcher);
//        System.out.println("Process:"+stat);
        //删除子节点
//        String fz = zk.create("/zk/zz","data02".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
//        zk.delete("/zk/zz0000000000", -1);
//		Stat stat2= zk.exists("/zk/zz0000000000", watcher);
//        System.out.println("process:"+stat2);

        // 关闭Zookeeper连接
        zk.close();
    }


}
