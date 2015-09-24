package gr.katsip.synefo.storm.api;

import org.apache.storm.zookeeper.*;
import org.apache.storm.zookeeper.AsyncCallback;
import org.apache.storm.zookeeper.CreateMode;
import org.apache.storm.zookeeper.KeeperException;
import org.apache.storm.zookeeper.WatchedEvent;
import org.apache.storm.zookeeper.Watcher;
import org.apache.storm.zookeeper.ZooDefs;
import org.apache.storm.zookeeper.ZooKeeper;
import org.apache.storm.zookeeper.data.Stat;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by nick on 9/23/15.
 */
public class ZookeeperClient {

    Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

    private static final String MAIN_ZNODE = "/synefo";

    private static final String TOPOLOGY_ZNODE = "physical";

    private static final String ACTIVE_TOPOLOGY_ZNODE = "active";

    private static final String TASK_ZNODE = "task";

    private static final String JOIN_STATE_ZNODE = "state";

    private ZooKeeper zookeeper;

    private String zookeeperAddress;

    private Integer identifier;

    private String taskName;

    private String taskAddress;

    public ZookeeperClient(String zookeeperAddress, String taskName, Integer identifier, String taskAddress) {
        this.zookeeperAddress = zookeeperAddress;
        this.taskName = taskName;
        this.identifier = identifier;
        this.taskAddress = taskAddress;
    }

    Watcher watcher = new Watcher() {

        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType().equals(Event.EventType.NodeDataChanged)) {

            }
        }
    };

    public void getScaleCommand() {
        String node = MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                taskName + ":" + identifier + "@" + taskAddress;
        try {
            zookeeper.getData(node, watcher, getCommandCallback, node.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private AsyncCallback.DataCallback getCommandCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (org.apache.zookeeper.KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    break;
                case NONODE:
                    break;
                case OK:
                    break;
                default:
            }
        }
    };

    public void init() {
        try {
            zookeeper = new ZooKeeper(zookeeperAddress, 100000, watcher);
            while (zookeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(10);
            }
            cleanup();
            create();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void create() {
        try {
            zookeeper.create(MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                    taskName + ":" + identifier + "@" + taskAddress, (MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                    taskName + ":" + identifier + "@" + taskAddress).getBytes("UTF-8"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zookeeper.create(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                            taskName + ":" + identifier + "@" + taskAddress, (MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                            taskName + ":" + identifier + "@" + taskAddress).getBytes("UTF-8"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void cleanup() {
        try {
            if (zookeeper.exists(MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                    taskName + ":" + identifier + "@" + taskAddress, false) != null) {
                zookeeper.delete(MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                        taskName + ":" + identifier + "@" + taskAddress, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
