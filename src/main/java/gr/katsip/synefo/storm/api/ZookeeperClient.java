package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.balancer.Util;
import org.apache.storm.zookeeper.AsyncCallback;
import org.apache.storm.zookeeper.CreateMode;
import org.apache.storm.zookeeper.KeeperException;
import org.apache.storm.zookeeper.WatchedEvent;
import org.apache.storm.zookeeper.Watcher;
import org.apache.storm.zookeeper.ZooDefs;
import org.apache.storm.zookeeper.ZooKeeper;
import org.apache.storm.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    private static final String SCALE_ACTION = "scale";

    private ZooKeeper zookeeper;

    private String zookeeperAddress;

    private Integer identifier;

    private String taskName;

    private String taskAddress;

    public ConcurrentLinkedQueue<String> commands;

    public ZookeeperClient(String zookeeperAddress, String taskName, Integer identifier, String taskAddress) {
        this.zookeeperAddress = zookeeperAddress;
        this.taskName = taskName;
        this.identifier = identifier;
        this.taskAddress = taskAddress;
        commands = new ConcurrentLinkedQueue<>();
    }

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType().equals(Event.EventType.NodeDataChanged)) {
                if (watchedEvent.getPath().equals(MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                        taskName + ":" + identifier)) {
                    getScaleCommand();
                }
            }
        }
    };

    public void getScaleCommand() {
        String node = MAIN_ZNODE + "/" + SCALE_ACTION + "/" + taskName + ":" + identifier;
        try {
            zookeeper.getData(node, watcher, getCommandCallback, node.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private AsyncCallback.DataCallback getCommandCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            String command = null;
            switch (org.apache.zookeeper.KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    logger.error("CONNECTIONLOSS");
                    getScaleCommand();
                    break;
                case NONODE:
                    logger.error("NONODE");
                    break;
                case OK:
                    try {
                        command = new String(bytes, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    if (command.length() > 0 && command.equals("") == false && command.lastIndexOf("/") < 0) {
                        logger.info("OK command: " + command);
                        commands.add(command);
                    }
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            org.apache.zookeeper.KeeperException.create(org.apache.zookeeper.KeeperException.Code.get(i), s));
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
                            taskName + ":" + identifier, (MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                            taskName + ":" + identifier).getBytes("UTF-8"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zookeeper.create(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                            taskName + ":" + identifier, (MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                            taskName + ":" + identifier).getBytes("UTF-8"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zookeeper.create(MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                            taskName + ":" + identifier, (MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                            taskName + ":" + identifier).getBytes("UTF-8"),
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
                    taskName + ":" + identifier, false) != null) {
                zookeeper.delete(MAIN_ZNODE + "/" + TASK_ZNODE + "/" +
                        taskName + ":" + identifier, -1);
            }
            if (zookeeper.exists(MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                    taskName + ":" + identifier, false) != null) {
                zookeeper.delete(MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                        taskName + ":" + identifier, -1);
            }
            if (zookeeper.exists(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                    taskName + ":" + identifier, false) != null) {
                zookeeper.delete(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" +
                        taskName + ":" + identifier, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ConcurrentHashMap<String, ArrayList<String>> getActiveTopology() throws UnsupportedEncodingException {
        Stat stat = new Stat();
        byte[] data = null;
        try {
            data = zookeeper.getData(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE, false, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("data received: " + new String(data, "UTF-8"));
        return Util.deserializeTopology(new String(data, "UTF-8"));
    }

    public List<String> getActiveDownstreamTasks() {
        logger.info("about to request downstream tasks for taskName: " + taskName + ", id: " + identifier);
        try {
            return getActiveTopology().get(taskName + ":" + identifier);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();
    }

    public List<Integer> getActiveDownstreamTaskIdentifiers() {
        List<String> taskNames = getActiveDownstreamTasks();
        List<Integer> taskIdentifiers = new ArrayList<>();
        for (String task : taskNames) {
            Integer identifier = Integer.parseInt(task.split("[:]")[1]);
            taskIdentifiers.add(identifier);
        }
        return taskIdentifiers;
    }

    public void addInputRateData(Double value) {
        byte[] b = new byte[8];
        ByteBuffer.wrap(b).putDouble(value);
        zookeeper.setData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + taskName + ":" + identifier, b, -1, statCallback, b);
    }

    private AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    logger.error(" CONNECTIONLOSS");
                    double value = ByteBuffer.wrap((byte[]) o).getDouble();
                    addInputRateData(value);
                    break;
                case NONODE:
                    logger.error(" NONODE");
                    break;
                case OK:
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            org.apache.zookeeper.KeeperException.create(org.apache.zookeeper.KeeperException.Code.get(i), s));
                    break;
            }
        }
    };

}
