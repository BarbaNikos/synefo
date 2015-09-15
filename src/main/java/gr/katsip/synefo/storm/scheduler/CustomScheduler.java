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
                if (!componentToExecutors.containsKey("order") || !componentToExecutors.containsKey("lineitem")) {
                    logger.info(this.getClass().getName() + " special components do not need scheduling.");
                }else {
                    List<ExecutorDetails> executors = componentToExecutors.get("order");
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails orderSupervisor = null, lineitemSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();
                        if (meta.get("name").equals("order")) {
                            orderSupervisor = supervisor;
                        }else if (meta.get("name").equals("lineitem")) {
                            lineitemSupervisor = supervisor;
                        }
                    }
                    if (orderSupervisor != null) {
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(orderSupervisor);

                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(orderSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(orderSupervisor.getId(), port));
                            }
                        }
                        availableSlots = cluster.getAvailableSlots(orderSupervisor);
                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
                    }
                    if (lineitemSupervisor != null) {
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(lineitemSupervisor);

                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(lineitemSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(lineitemSupervisor.getId(), port));
                            }
                        }
                        availableSlots = cluster.getAvailableSlots(lineitemSupervisor);
                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
                    }
                }
            }
        }
        new EvenScheduler().schedule(topologies, cluster);
    }
}
