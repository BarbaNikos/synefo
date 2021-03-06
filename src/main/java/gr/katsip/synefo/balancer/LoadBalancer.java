package gr.katsip.synefo.balancer;

import gr.katsip.synefo.utils.GenericTriplet;
import gr.katsip.synefo.utils.Pair;
import gr.katsip.synefo.utils.JoinOperator;
import gr.katsip.synefo.utils.Util;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private int activeTopologyVersion;

    private final ReadWriteLock writeLock = new ReentrantReadWriteLock();

    private int SCALE_ACTION_COMPLETE_VERSION = -1;

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                if (watchedEvent.getPath().equals(MAIN_ZNODE + "/" + TASK_ZNODE)) {
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
                    System.out.println("taskChildrenCallback CONNECTIONLOSS");
                    try {
                        watchTaskNode(new String((byte[]) o, "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                case NONODE:
                    System.out.println("taskChildrenCallback NONODE");
                    break;
                case OK:
                    list.removeAll(registeredTasks);
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
                    System.out.println("taskChildrenCallback CONNECTIONLOSS");
                    getTaskData(s);
                    break;
                case NONODE:
                    System.out.println("taskChildrenCallback NONODE");
                    break;
                case OK:
                    String taskName = s.substring(s.lastIndexOf('/') + 1, s.lastIndexOf(':'));
                    Integer identifier = Integer.parseInt(s.substring(s.lastIndexOf(':') + 1));
                    System.out.println("thread-" + Thread.currentThread().getId() + " identified data point from " +
                            taskName + ":" + identifier + ".");
//                    double value = ByteBuffer.wrap(bytes).getDouble();
                    String data = ByteBuffer.wrap(bytes).toString();
                    for (String dataPoint : data.split(",")) {
                        double value = Double.parseDouble(dataPoint);
                        scaleFunction.addInputRateDataBatch(taskName, identifier, value);
                    }
//                    String taskName = s.substring(s.lastIndexOf('/') + 1, s.lastIndexOf(':'));
//                    Integer identifier = Integer.parseInt(s.substring(s.lastIndexOf(':') + 1));
//                    System.out.println("thread-" + Thread.currentThread().getId() + " identified data point from " +
//                            taskName + ":" + identifier + " and the data is " + value);
//                    GenericTriplet<String, String, String> action = scaleFunction.addData(taskName, Integer.parseInt(s.substring(s.lastIndexOf(':') + 1)),
//                            Double.parseDouble(data[0]), Double.parseDouble(data[1]),
//                            Double.parseDouble(data[2]), Double.parseDouble(data[3]), Double.parseDouble(data[4]));
                    writeLock.writeLock().lock();
//                    GenericTriplet<String, String, String> action = scaleFunction.addInputRateData(
//                            taskName, identifier, value);
                    GenericTriplet<String, String, String> action = scaleFunction.scaleCheckAfterBatch(taskName, identifier);
                    if (action.first != null) {
                        System.out.println("thread-" + Thread.currentThread().getId() + " action generated " +
                                action.first + " for upstream task: " + action.second + " directed to: " +
                                action.third);
                        initializeTaskCompletionStatus(action.third);
                        if (scaleFunction.getTaskToRelationIndex().get(action.third).getStep().equals(JoinOperator.Step.JOIN))
                            initializeStateNode(action.third);
                        /**
                         * Need to (1) update active-topology
                         */
//                        System.out.println("LoadBalancer: active-topology before addition/removal of task: " +
//                                scaleFunction.getActiveTopology().toString());
                        switch (action.first) {
                            case "add":
                                scaleFunction.activateTask(action.third);
//                                System.out.println("LoadBalancer: about to activate task: " + action.third);
//                                if (!scaleFunction.getActiveTopology().containsKey(action.third)) {
//                                    System.out.println("thread-" + Thread.currentThread().getId() +
//                                            " Error in updating active-topology (add)");
//                                    System.exit(1);
//                                }
//                                System.out.println("LoadBalancer: active-topology after addition of task: " + scaleFunction.getActiveTopology().toString());
                                break;
                            case "remove":
                                scaleFunction.deactivateTask(action.third);
//                                System.out.println("LoadBalancer: about to de-activate task: " + action.third);
//                                if (scaleFunction.getActiveTopology().containsKey(action.third)) {
//                                    System.out.println("thread-" + Thread.currentThread().getId() +
//                                            " Error in updating active-topology (remove)");
//                                    System.exit(1);
//                                }
//                                System.out.println("LoadBalancer: active-topology after removal of task: " + scaleFunction.getActiveTopology().toString());
                                break;
                        }
                        setActiveTopology(new ConcurrentHashMap<>(scaleFunction.getActiveTopology()));
                        /**
                         * 2) set the commands in the /synefo/bolt-tasks
                         * Add/Remove and Activate/Deactivate
                         */
                        List<String> parentTasks = Util.getInverseTopology(getTopology()).get(action.third);
//                        System.out.println("thread-" + Thread.currentThread().getId() + " parent-tasks of node " +
//                                action.third + " are: " + parentTasks.toString());
                        parentTasks.remove(parentTasks.indexOf(action.second));
                        if (action.first.equals("add")) {
                            for (String task : parentTasks)
                                setScaleAction(task, "activate", action.third);
                            setScaleAction(action.second, action.first, action.third);
                        } else if (action.first.equals("remove")) {
                            for (String task : parentTasks)
                                setScaleAction(task, "deactivate", action.third);
                            setScaleAction(action.second, action.first, action.third);
                        }
                        while (waitForScaleAction(action.third)) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    writeLock.writeLock().unlock();
                    break;
                default:
                    logger.error("unexpected scenario: " +
                            KeeperException.create(KeeperException.Code.get(i), s));
                    break;
            }
        }
    };

    private void initializeStateNode(String task) {
        Stat stat = null;
        try {
            stat = zooKeeper.setData(MAIN_ZNODE + "/" + JOIN_STATE_ZNODE + "/" + task, ("").getBytes("UTF-8"), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void initializeTaskCompletionStatus(String task) {
        Stat stat = null;
        try {
            stat = zooKeeper.setData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + task, ("").getBytes("UTF-8"), -1);
            SCALE_ACTION_COMPLETE_VERSION = stat.getVersion();
            System.out.println("LoadBalancer: initiated scale action with version: " + SCALE_ACTION_COMPLETE_VERSION);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private boolean waitForScaleAction(String task) {
        Stat stat = new Stat();
        try {
            byte[] data = zooKeeper.getData(MAIN_ZNODE + "/" + TASK_ZNODE + "/" + task, false, stat);
            String message = new String(data, "UTF-8");
            if (message.equals("DONE")) {
                if (stat.getVersion() <= SCALE_ACTION_COMPLETE_VERSION && SCALE_ACTION_COMPLETE_VERSION != -1) {
                    System.out.println("LoadBalancer: something might have gone WRONG. Previous update version: " +
                    SCALE_ACTION_COMPLETE_VERSION + ", retrieved version: " + stat.getVersion());
                }
                SCALE_ACTION_COMPLETE_VERSION = -1;
                return true;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return false;
    }

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
                    System.out.println("LoadBalancer: set scale action version" + stat.getVersion() + " for path: " + s);
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
        this.activeTopologyVersion = -1;
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

    public void setActiveTopology(final ConcurrentHashMap<String, ArrayList<String>> activeTopology) {
        Stat stat = null;
        try {
            stat = zooKeeper.setData(MAIN_ZNODE + "/" + ACTIVE_TOPOLOGY_ZNODE,
                    Util.serializeTopology(activeTopology).getBytes("UTF-8"), -1);
            if (stat.getVersion() > activeTopologyVersion) {
                activeTopologyVersion = stat.getVersion();
                System.out.println("LoadBalancer: updated active topology to version " + activeTopologyVersion);
            }
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
            System.out.println("LoadBalancer: retrieved active topology version: " + stat.getVersion());
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
            zooKeeper.create(MAIN_ZNODE, ("").getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        recursiveDelete(MAIN_ZNODE);
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
