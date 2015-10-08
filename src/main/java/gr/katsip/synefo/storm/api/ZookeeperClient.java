package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.utils.Util;
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

    public ConcurrentLinkedQueue<String> commands;

    private int commandVersion;

    private int activeTopologyVersion;

    private String elasticTask = "";

    private String scaleAction = "";

    private StringBuilder serializedStatistics;

    private int statisticCounter;

    private List<String> scaleResult = null;

    public List<String> getScaleResult() {
        return scaleResult;
    }

    public void clearActionData() {
        if (scaleResult != null)
            scaleResult.clear();
        scaleResult = null;
        elasticTask = "";
        scaleAction = "";
    }

    public ZookeeperClient(String zookeeperAddress, String taskName, Integer identifier, String taskAddress) {
        this.zookeeperAddress = zookeeperAddress;
        this.taskName = taskName;
        this.identifier = identifier;
        commands = new ConcurrentLinkedQueue<>();
        commandVersion = -1;
        activeTopologyVersion = -1;
        serializedStatistics = new StringBuilder();
        statisticCounter = 0;
    }

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType().equals(Event.EventType.NodeDataChanged)) {
                if (watchedEvent.getPath().equals(MAIN_ZNODE + "/" + SCALE_ACTION + "/" +
                        taskName + ":" + identifier)) {
                    getScaleCommand();
                }else if (watchedEvent.getPath().contains(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/")) {
                    if (scaleAction.equals("") == false && elasticTask.equals("") == false)
                        getJoinState(scaleAction, elasticTask);
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
                    if (stat.getVersion() >= commandVersion) {
                        if (command.length() > 0 && command.equals("") == false && command.lastIndexOf("/") < 0) {
                            logger.info("OK command: " + command);
                            commands.add(command);
                            commandVersion = stat.getVersion();
                        }
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

    public ConcurrentHashMap<String, ArrayList<String>> getActiveTopology() {
        Stat stat = new Stat();
        byte[] data = null;
        try {
            data = zookeeper.getData(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE, false, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (stat.getVersion() > activeTopologyVersion) {
            activeTopologyVersion = stat.getVersion();
        }
        try {
            logger.info("data received (version:" + activeTopologyVersion + ": " + new String(data, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            return Util.deserializeTopology(new String(data, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ConcurrentHashMap<String, ArrayList<String>> getTopology() throws UnsupportedEncodingException {
        Stat stat = new Stat();
        byte[] data = null;
        try {
            data = zookeeper.getData(MAIN_ZNODE + "/" + TOPOLOGY_ZNODE, false, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("data received: " + new String(data, "UTF-8"));
        return Util.deserializeTopology(new String(data, "UTF-8"));
    }

    public List<String> getDownstreamTasks() {
        logger.info("about to request downstream tasks for taskName: " + taskName + ", id: " + identifier);
        try {
            return getTopology().get(taskName + ":" + identifier);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();
    }

    public List<String> getActiveDownstreamTasks() {
        logger.info("about to request downstream tasks for taskName: " + taskName + ", id: " + identifier);
        ConcurrentHashMap<String, ArrayList<String>> activeTopology = getActiveTopology();
        if (activeTopology != null) {
            return activeTopology.get(taskName + ":" + identifier);
        }else {
            return new ArrayList<String>();
        }
    }

    public List<Integer> getDownstreamTaskIdentifiers() {
        List<String> taskNames = getDownstreamTasks();
        if (taskNames.size() > 0) {
            List<Integer> taskIdentifiers = new ArrayList<>();
            for (String task : taskNames) {
                Integer identifier = Integer.parseInt(task.split("[:]")[1]);
                taskIdentifiers.add(identifier);
            }
            return taskIdentifiers;
        }else {
            return new ArrayList<Integer>();
        }
    }

    public List<Integer> getActiveDownstreamTaskIdentifiers() {
        List<String> taskNames = getActiveDownstreamTasks();
        if (taskNames.size() > 0) {
            List<Integer> taskIdentifiers = new ArrayList<>();
            for (String task : taskNames) {
                Integer identifier = Integer.parseInt(task.split("[:]")[1]);
                taskIdentifiers.add(identifier);
            }
            return taskIdentifiers;
        }else {
            return new ArrayList<Integer>();
        }
    }

    public void addInputRateData(Double value) {
//        logger.info("adding a data point to the buffer");
        statisticCounter++;
        serializedStatistics.append("," + value);
        if (statisticCounter >= Util.BOLT_STAT_BATCH_SIZE) {
//            logger.info("stat buffer is full. Time to push");
            try {
                zookeeper.setData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + taskName + ":" + identifier,
                        serializedStatistics.toString().getBytes("UTF-8"), -1, statCallback,
                        serializedStatistics.toString().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            statisticCounter = 0;
            serializedStatistics = new StringBuilder();
        }
//        byte[] b = new byte[8];
//        ByteBuffer.wrap(b).putDouble(value);
//        zookeeper.setData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + taskName + ":" + identifier, b, -1, statCallback, b);
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
//                    logger.info(" successfully pushed statistic data (version: " + stat.getVersion() + ")");
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            org.apache.zookeeper.KeeperException.create(org.apache.zookeeper.KeeperException.Code.get(i), s));
                    break;
            }
        }
    };

    public void notifyActionComplete() {
        try {
            zookeeper.setData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + taskName + ":" + identifier, ("DONE").getBytes("UTF-8"), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void getJoinState(String action, String taskWithIdentifier) {
        if (scaleAction != "" || elasticTask != "")
            logger.error(" something went wrong and scale-action (" + scaleAction + ") and elastic-task (" +
                    elasticTask + ") are not initialized.");
        this.scaleAction = action;
        this.elasticTask = taskWithIdentifier;
        try {
            zookeeper.getData(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" + elasticTask, watcher,
                    joinStateCallback,
                    (scaleAction + "," + elasticTask).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void setJoinState(String taskName, Integer identifier, List<String> keys) {
        try {
            zookeeper.setData(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" + taskName + ":" + identifier, Util.serializeList(keys).getBytes("UTF-8"), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private AsyncCallback.DataCallback joinStateCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (org.apache.zookeeper.KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    logger.info("CONNECTIONLOSS");
                    try {
                        String context = new String((byte[]) o, "UTF-8");
                        getJoinState(context.split(",")[0], context.split(",")[1]);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                case NONODE:
                    logger.info("NONODE");
                    break;
                case OK:
                    try {
                        String data = new String(bytes, "UTF-8");
                        if (data.equals("") == false) {
                            scaleResult = Util.deserializeList(new String(bytes, "UTF-8"));
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    logger.error("unexpected scenario: " +
                        org.apache.zookeeper.KeeperException.create(org.apache.zookeeper.KeeperException.Code.get(i), s));
                    break;
            }
        }
    };
}
