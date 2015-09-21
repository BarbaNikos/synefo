package gr.katsip.synefo.storm.scheduler;

import backtype.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by katsip on 9/15/2015.
 */
public class CustomScheduler implements IScheduler {

    Logger logger = LoggerFactory.getLogger(CustomScheduler.class);

    private String[] spoutName = { "order", "lineitem" };

    @Override
    public void prepare(Map map) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        TopologyDetails topology = topologies.getByName("join-dispatch");
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);
            if (!needsScheduling) {
                logger.info(this.getClass().getName() + " no scheduling is needed.");
            }else {
                logger.info(this.getClass().getName() + " scheduling is needed.");
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster
                        .getNeedsSchedulingComponentToExecutors(topology);
                logger.info(this.getClass().getName() + " components for scheduling: " + componentToExecutors);
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(
                        topologies.getByName("join-dispatch").getId());
                if (currentAssignment != null) {
                    logger.info(this.getClass().getName() + " current assignments: " +
                            currentAssignment.getExecutorToSlot());
                }else {
                    logger.info(this.getClass().getName() + " current assignments: {}");
                }
                if (!componentToExecutors.containsKey("order") || !componentToExecutors.containsKey("lineitem") ||
                        !componentToExecutors.containsKey("dispatch")) {
                    logger.info(this.getClass().getName() + " special components do not need scheduling.");
                }else {
                    List<ExecutorDetails> orderExecutors = componentToExecutors.get("order");
                    List<ExecutorDetails> lineitemExecutors = componentToExecutors.get("lineitem");
                    List<ExecutorDetails> dispatchExecutors = componentToExecutors.get("dispatch");
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails orderSupervisor = null, lineitemSupervisor = null, dispatchSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        if (supervisor.getSchedulerMeta() != null) {
                            Map meta = (Map) supervisor.getSchedulerMeta();
                            if (meta.get("name").equals("order")) {
                                orderSupervisor = supervisor;
                            }
                            if (meta.get("name").equals("lineitem")) {
                                lineitemSupervisor = supervisor;
                            }
                            if (meta.get("name").equals("supervisor")) {
                                dispatchSupervisor = supervisor;
                            }
                        }
                    }
                    if (orderSupervisor != null) {
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(orderSupervisor);
                        if (availableSlots.isEmpty() && !orderExecutors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(orderSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(orderSupervisor.getId(), port));
                            }
                        }
                        availableSlots = cluster.getAvailableSlots(orderSupervisor);
                        cluster.assign(availableSlots.get(0), topology.getId(), orderExecutors);
                    }
                    if (lineitemSupervisor != null) {
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(lineitemSupervisor);
                        if (availableSlots.isEmpty() && !lineitemExecutors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(lineitemSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(lineitemSupervisor.getId(), port));
                            }
                        }
                        availableSlots = cluster.getAvailableSlots(lineitemSupervisor);
                        cluster.assign(availableSlots.get(0), topology.getId(), lineitemExecutors);
                    }
                    if (dispatchSupervisor != null) {
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(dispatchSupervisor);
                        if (availableSlots.isEmpty() && !dispatchExecutors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(dispatchSupervisor)) {
                                cluster.freeSlot((new WorkerSlot(dispatchSupervisor.getId(), port)));
                            }
                        }
                        availableSlots = cluster.getAvailableSlots(dispatchSupervisor);
                        cluster.assign(availableSlots.get(0), topology.getId(), dispatchExecutors);
                    }
                }
            }
        }
        new EvenScheduler().schedule(topologies, cluster);
    }
}
