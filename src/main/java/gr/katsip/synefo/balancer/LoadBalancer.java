package gr.katsip.synefo.balancer;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.storm.api.GenericTriplet;
import gr.katsip.synefo.storm.api.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by katsip on 9/22/2015.
 */
public class LoadBalancer {

    private final ConcurrentHashMap<String, String> taskAddressIndex;

    Logger logger = LoggerFactory.getLogger(LoadBalancer.class);

    private ZooKeeper zooKeeper;

    private String zookeeperAddress;

    List<String> registeredTasks = null;

    private static final String MAIN_ZNODE = "/synefo";

    private static final String TOPOLOGY_ZNODE = "physical";

    private static final String ACTIVE_TOPOLOGY_ZNODE = "active";

    private static final String TASK_ZNODE = "task";

    private static final String SCALE_ACTION = "scale";

    private static final String JOIN_STATE_ZNODE = "state";

    private NewScaleFunction scaleFunction;

    Watcher watcher = new Watcher() {

        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                if (watchedEvent.getPath().equals(MAIN_ZNODE + "/" + TASK_ZNODE)) {
                    /**
                     * New task registered
                     */
//                    logger.info("identified NodeChildrenChanged event on " + watchedEvent.getPath());
                    System.out.println("identified NodeChildrenChanged event on " + watchedEvent.getPath());
                    watchTaskNode(watchedEvent.getPath());
                }
            }else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                getTaskData(watchedEvent.getPath());
            }
        }
    };

    private AsyncCallback.Children2Callback taskChildrenCallback = new AsyncCallback.Children2Callback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
//                    logger.error("taskChildrenCallback CONNECTIONLOSS");
                    System.out.println("taskChildrenCallback CONNECTIONLOSS");
                    try {
                        watchTaskNode(new String((byte[]) o, "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                case NONODE:
//                    logger.error("taskChildrenCallback NONODE");
                    System.out.println("taskChildrenCallback NONODE");
                    break;
                case OK:
                    list.removeAll(registeredTasks);
//                    logger.info("taskChildrenCallback OK: new nodes: " + registeredTasks.toString());
                    System.out.println("taskChildrenCallback OK: new nodes: " + registeredTasks.toString());
                    for (String child : list) {
                        getTaskData(s + "/" + child);
                    }
                    registeredTasks.addAll(list);
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            KeeperException.create(KeeperException.Code.get(i), s));
                    break;
            }
        }
    };

    private AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
//                    logger.error("taskChildrenCallback CONNECTIONLOSS");
                    System.out.println("taskChildrenCallback CONNECTIONLOSS");
                    getTaskData(s);
                    break;
                case NONODE:
//                    logger.error("taskChildrenCallback NONODE");
                    System.out.println("taskChildrenCallback NONODE");
                    break;
                case OK:
                    double value = ByteBuffer.wrap(bytes).getDouble();
                    String taskName = s.substring(s.lastIndexOf('/') + 1, s.lastIndexOf(':'));
                    Integer identifier = Integer.parseInt(s.substring(s.lastIndexOf(':') + 1));
                    String taskAddress = taskAddressIndex.get(taskName + ":" + identifier);
                    System.out.println("identified data point from " + taskName + ":" + identifier + " and the data is " + value);
//                    GenericTriplet<String, String, String> action = scaleFunction.addData(taskName, Integer.parseInt(s.substring(s.lastIndexOf(':') + 1)),
//                            Double.parseDouble(data[0]), Double.parseDouble(data[1]),
//                            Double.parseDouble(data[2]), Double.parseDouble(data[3]), Double.parseDouble(data[4]));
                    GenericTriplet<String, String, String> action = scaleFunction.addInputRateData(
                            taskName, identifier, value);
                    if (action.first != null) {
                        System.out.println("action generated " + action.first + " for upstream task: " +
                                action.second + " directed to: " + action.third);
                        /**
                         * Need to
                         * 1) update active-topology
                         */
                        switch (action.first) {
                            case "add":
                                scaleFunction.activateTask(action.third);
                                break;
                            case "remove":
                                scaleFunction.deactivateTask(action.third);
                                break;
                        }
                        setActiveTopology(
                                new ConcurrentHashMap<String, ArrayList<String>>(scaleFunction.getActiveTopology()));
                        /**
                         * 2) set the commands in the /synefo/bolt-tasks
                         * Add/Remove and Activate/Deactivate
                         */
                        List<String> parentTasks = Util.getInverseTopology(getTopology()).get(action.third);
                        System.out.println("parent-tasks of node " + action.third + " are: " + parentTasks.toString());
                        parentTasks.remove(parentTasks.indexOf(action.second));
                        if (action.first.equals("add")) {
                            for (String task : parentTasks) {
                                setScaleAction(task, "activate", action.third);
                            }
                            setScaleAction(action.second, action.first, action.third);
                        }else if (action.first.equals("remove")) {
                            for (String task : parentTasks) {
                                setScaleAction(task, "deactivate", action.third);
                            }
                            setScaleAction(action.second, action.first, action.third);
                        }
                    }
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            KeeperException.create(KeeperException.Code.get(i), s));
                    break;
            }
        }
    };
    private AsyncCallback.StatCallback setActionCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            String[] tokens = new String[0];
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    System.out.println("CONNECTIONLOSS");
                    try {
                        tokens = (new String((byte[]) o, "UTF-8")).split(",");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    setScaleAction(tokens[0], tokens[1], tokens[2]);
                    break;
                case NONODE:
                    System.out.println("NONODE");
                    try {
                        tokens = (new String((byte[]) o, "UTF-8")).split(",");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    setScaleAction(tokens[0], tokens[1], tokens[2]);
                    break;
                case OK:
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            KeeperException.create(KeeperException.Code.get(i), s));
                    break;
            }
        }
    };

    private void setScaleAction(String taskWithIdentifier, String action, String targetTaskWithIdentifier) {
        try {
            zooKeeper.setData(MAIN_ZNODE + "/" + SCALE_ACTION + "/" + taskWithIdentifier,
                    (action + "~" + targetTaskWithIdentifier + "@" +
                            taskAddressIndex.get(targetTaskWithIdentifier)).getBytes("UTF-8"), -1,
                    setActionCallback, (taskWithIdentifier + "," + action + "," + targetTaskWithIdentifier).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void getTaskData(String taskNode) {
        try {
            zooKeeper.getData(taskNode, watcher, taskDataCallback, taskNode.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void watchTaskNode(String taskNode) {
        try {
            zooKeeper.getChildren(taskNode, true, taskChildrenCallback, taskNode.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public LoadBalancer(String zookeeperAddress,
                         ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation,
                        Map<String, Pair<Number, Number>> thresholds,
                        ConcurrentHashMap<String, String> taskAddressIndex) {
        this.zookeeperAddress = zookeeperAddress;
        scaleFunction = new NewScaleFunction(new ConcurrentHashMap<>(taskToJoinRelation), thresholds);
        this.taskAddressIndex = taskAddressIndex;
    }

    public void start() {
        try {
            zooKeeper = new ZooKeeper(zookeeperAddress, 100000, watcher);
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        cleanup();
        nodeInitiatialize();
        registeredTasks = new CopyOnWriteArrayList<>();
        watchTaskNode(MAIN_ZNODE + "/" + TASK_ZNODE);
    }

    public void updateScaleFunctionTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
        scaleFunction.updateTopology(topology);
    }

    public void setTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
        try {
            zooKeeper.setData(MAIN_ZNODE + "/" + TOPOLOGY_ZNODE,
                    Util.serializeTopology(topology).getBytes("UTF-8"), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public ConcurrentHashMap<String, ArrayList<String>> getTopology() {
        Stat stat = new Stat();
        try {
            String data = new String(zooKeeper.getData(MAIN_ZNODE + "/" + TOPOLOGY_ZNODE, false, stat), "UTF-8");
            return Util.deserializeTopology(data);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void updateScaleFunctionActiveTopology(ConcurrentHashMap<String, ArrayList<String>> activeTopology) {
        scaleFunction.updateActiveTopology(activeTopology);
    }

    public void setActiveTopology(ConcurrentHashMap<String, ArrayList<String>> activeTopology) {
        try {
            zooKeeper.setData(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE,
                    Util.serializeTopology(activeTopology).getBytes("UTF-8"), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public ConcurrentHashMap<String, ArrayList<String>> getActiveTopology() {
        Stat stat = new Stat();
        try {
            String data = new String(zooKeeper.getData(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE, false, stat), "UTF-8");
            return Util.deserializeTopology(data);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void nodeInitiatialize() {
        try {
            zooKeeper.create(MAIN_ZNODE + "/" + TOPOLOGY_ZNODE,
                    ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE,
                    ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create(MAIN_ZNODE + "/" + TASK_ZNODE,
                    ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE,
                    ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create(MAIN_ZNODE + "/" + SCALE_ACTION,
                    ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void cleanup() {
        recursiveDelete(MAIN_ZNODE + "/" + TOPOLOGY_ZNODE);
        recursiveDelete(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE);
        recursiveDelete(MAIN_ZNODE + "/" + TASK_ZNODE);
        recursiveDelete(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE);
        recursiveDelete(MAIN_ZNODE + "/" + SCALE_ACTION);
    }

    private void recursiveDelete(String znode) {
        try {
            if (zooKeeper.exists(znode, false) != null) {
                List<String> children = zooKeeper.getChildren(znode, false);
                if (children != null && children.size() > 0) {
                    for (String child : children) {
                        if (zooKeeper.getChildren(znode + "/" + child, false) != null &&
                                zooKeeper.getChildren(znode + "/" + child, false).size() > 0)
                            recursiveDelete(znode + "/" + child);
                        zooKeeper.delete(znode + "/" + child, -1);
                    }
                }
                zooKeeper.delete(znode, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
