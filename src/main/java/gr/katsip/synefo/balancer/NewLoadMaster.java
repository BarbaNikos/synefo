package gr.katsip.synefo.balancer;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.server2.ScaleFunction;
import gr.katsip.synefo.storm.api.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by nick on 9/22/15.
 */
public class NewLoadMaster implements Runnable {

    Logger logger = LoggerFactory.getLogger(NewLoadMaster.class);

    private ConcurrentHashMap<String, ArrayList<String>> topology;

    private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

    private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

    private ConcurrentHashMap<String, String> taskAddressIndex;

    private LoadBalancer balancer;

    private HashMap<String, Pair<Number, Number>> resourceThresholds;

    private String zookeeperAddress;

    private AtomicInteger taskNumber = null;

    private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;

    public NewLoadMaster(String zookeeperAddress,
                         HashMap<String, Pair<Number, Number>> resourceThresholds,
                         ConcurrentHashMap<String, ArrayList<String>> topology,
                         ConcurrentHashMap<String, ArrayList<String>> activeTopology,
                         ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                         ConcurrentHashMap<String, String> taskAddressIndex,
                         AtomicInteger taskNumber,
                         ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
        this.topology = topology;
        this.activeTopology = activeTopology;
        this.taskIdentifierIndex = taskIdentifierIndex;
        this.taskAddressIndex = taskAddressIndex;
        this.zookeeperAddress = zookeeperAddress;
        this.taskNumber = taskNumber;
        this.taskToJoinRelation = taskToJoinRelation;
        this.resourceThresholds = resourceThresholds;
    }


    @Override
    public void run() {
        while (this.taskNumber.get() == -1) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (taskIdentifierIndex.size() < taskNumber.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        balancer = new LoadBalancer(zookeeperAddress, taskToJoinRelation, resourceThresholds);
        balancer.start();
        ConcurrentHashMap<String, ArrayList<String>> expandedTopology = Util.topologyTaskExpand(taskIdentifierIndex,
                topology);
        ConcurrentHashMap<String, ArrayList<String>> finalTopology = Util.updateTopology(taskAddressIndex, taskIdentifierIndex,
                expandedTopology);
        topology.clear();
        topology.putAll(finalTopology);
        activeTopology.clear();
        activeTopology.putAll(Util.getInitialActiveTopologyWithJoinOperators(topology,
                Util.getInverseTopology(topology), taskToJoinRelation, true));
        logger.info("finalized topology: " + topology.toString());
        logger.info("finalized active topology: " + topology.toString());
        taskIdentifierIndex.clear();
        balancer.setTopology(topology);
        balancer.setTopology(activeTopology);
        logger.info(" initiated the whole process.");
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
