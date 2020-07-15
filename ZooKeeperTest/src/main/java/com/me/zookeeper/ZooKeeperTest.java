package com.me.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author maow
 * @create 2020-04-21 20:13
 */
public class ZooKeeperTest {
    private ZooKeeper zkClient;
    @Before
    public void before() throws IOException {
        String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
            }
        });
    }

    @After
    public void after() throws InterruptedException {
        zkClient.close();
    }

    @Test
    public void create() throws IOException, KeeperException, InterruptedException {
        String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        ZooKeeper zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
            }
        });
        zkClient.create("/maow","maowei".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getNodeData() throws IOException, KeeperException, InterruptedException {
        String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        ZooKeeper zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("YEAH");
            }
        });
        Stat stat = zkClient.exists("/maow", false);
        if (stat == null){
            System.out.println("节点不存在");
        }else {
            byte[] data = zkClient.getData("/maow", false, stat);
            System.out.println(new String(data));
        }
    }


    @Test
    public void getChildrenNode() throws IOException, KeeperException, InterruptedException {
        String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        ZooKeeper zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
            }
        });
        List<String> children = zkClient.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("子节点发生变化");
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void setNodeDta() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/maow", false);
        if (stat == null){
            System.out.println("节点不存在");
        }else {
            zkClient.setData("/maow", "MAOWEI".getBytes(), stat.getVersion());//乐观锁
        }
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/abc", false);
        if (stat == null){
            System.out.println("节点不存在");
        }else {
            System.out.println(stat);
            zkClient.delete("/abc",stat.getVersion());
        }
    }
}
