package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by katsip on 9/11/2015.
 */
public class NewJoinDispatcher implements Serializable {

    Logger logger = LoggerFactory.getLogger(NewJoinDispatcher.class);

    private List<Values> state;

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private Fields resultSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private String outerRelationForeignKey;

    private String innerRelationForeignKey;

    private HashMap<String, List<Integer>> outerRelationIndex;

    private HashMap<String, List<Integer>> innerRelationIndex;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    private Fields outputSchema;

    public NewJoinDispatcher(String outerRelationName, Fields outerRelationSchema,
                             String outerRelationKey, String outerRelationForeignKey,
                             String innerRelationName, Fields innerRelationSchema,
                             String innerRelationKey, String innerRelationForeignKey, Fields outputSchema) {
        this.outerRelationName = outerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.outerRelationKey = outerRelationKey;
        this.outerRelationForeignKey = outerRelationForeignKey;
        this.innerRelationName = innerRelationName;
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.innerRelationForeignKey = innerRelationForeignKey;
        outerRelationIndex = new HashMap<>();
        innerRelationIndex = new HashMap<>();
        taskToRelationIndex = null;
        this.outputSchema = new Fields(outputSchema.toList());
    }

    /**
     * add task to relation indices inside the Dispatcher's structures. This method needs to be called when a task is
     * added.
     * @param relationName the relation to which the operator is added
     * @param originalTask the originally active task
     * @param newTask the newly added operator
     */
    public void expandTask(String relationName, Integer originalTask, Integer newTask) {
        Iterator<Map.Entry<String, List<Integer>>> iterator = null;
        HashMap<String, List<Integer>> indexCopy = null;
        if (relationName.equals(outerRelationName)) {
           iterator = outerRelationIndex.entrySet().iterator();
            indexCopy = new HashMap<>(outerRelationIndex);
        }else {
            iterator = innerRelationIndex.entrySet().iterator();
            indexCopy = new HashMap<>(innerRelationIndex);
        }
        String key = "";
        List<Integer> taskList = null;
        while (iterator != null && iterator.hasNext()) {
            Map.Entry<String, List<Integer>> pair = iterator.next();
            taskList = pair.getValue();
            if (taskList.lastIndexOf(originalTask) != -1 && taskList.lastIndexOf(originalTask) != 0) {
                key = pair.getKey();
                taskList.add(newTask);
                taskList.set(0, 1);
                indexCopy.put(key, new ArrayList<Integer>(taskList));
            }
        }
        if (relationName.equals(outerRelationName)) {
            outerRelationIndex.clear();
            outerRelationIndex.putAll(indexCopy);
        }else {
            innerRelationIndex.clear();
            innerRelationIndex.putAll(indexCopy);
        }
        List<Integer> tasks = taskToRelationIndex.get(relationName);
        tasks.add(newTask);
        taskToRelationIndex.put(relationName, tasks);
    }

    /**
     * remove task from relation indices inside the Dispatcher's structures. This method needs to be called when a task
     * is removed.
     * @param relationName the relation from which the operator belongs to
     * @param remainingTask the remaining active operator
     * @param removedTask the removed operator
     */
    public void mergeTask(String relationName, Integer remainingTask, Integer removedTask) {
        Iterator<Map.Entry<String, List<Integer>>> iterator = null;
        HashMap<String, List<Integer>> indexCopy = null;
        if (relationName.equals(outerRelationName)) {
            iterator = outerRelationIndex.entrySet().iterator();
            indexCopy = new HashMap<>(outerRelationIndex);
        }else {
            iterator = innerRelationIndex.entrySet().iterator();
            indexCopy = new HashMap<>(innerRelationIndex);
        }
        String key = "";
        List<Integer> taskList = null;
        while (iterator != null && iterator.hasNext()) {
            Map.Entry<String, List<Integer>> pair = iterator.next();
            taskList = pair.getValue();
            if (taskList.lastIndexOf(removedTask) != -1 && taskList.lastIndexOf(removedTask) != 0) {
                key = pair.getKey();
                taskList.remove(taskList.lastIndexOf(removedTask));
                taskList.set(0, 1);
                indexCopy.put(key, taskList);
            }
        }
        if (relationName.equals(outerRelationName)) {
            outerRelationIndex.clear();
            outerRelationIndex.putAll(indexCopy);
        }else {
            innerRelationIndex.clear();
            innerRelationIndex.putAll(indexCopy);
        }
        List<Integer> tasks = taskToRelationIndex.get(relationName);
        tasks.remove(tasks.lastIndexOf(removedTask));
        taskToRelationIndex.put(relationName, tasks);
    }

    /**
     * This function initializes the internal structure which keeps track of the active tasks and in
     * which relation they belong to.
     * @caution Need to have the ACTIVE tasks!
     * @param taskToRelationIndex the Hash-map with the initially active tasks and the relation they belong to.
     */
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex) {
        this.taskToRelationIndex = new HashMap<>(taskToRelationIndex);
    }

    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    private int dispatch(String primaryKey, String foreignKey, HashMap<String, List<Integer>> primaryRelationIndex,
                        String primaryRelationName, HashMap<String, List<Integer>> secondaryRelationIndex,
                        Fields attributeNames, Values attributeValues, OutputCollector collector, Tuple anchor) {
//        logger.info("dispatch() called primary-key: " + primaryKey + ", foreign-key: " + foreignKey +
//        " primary-relation: " + primaryRelationName + ", attributes: " + attributeNames.toList().toString() +
//        " values: " + attributeValues.toString());
        /**
         * STORE:
         * 2 cases: (a) primary-key has been encountered before (b) primary-key has not been encountered before
         * case (a): Send it to the next available operator
         * case (b): Pick one of the active nodes randomly and send the key there.
         *              In case of (b), all the other nodes
         *              that share common keys with the selected node, will receive same keys also
         */
        if (primaryRelationIndex.containsKey(primaryKey)) {
            List<Integer> dispatchInfo = primaryRelationIndex.get(primaryKey);
            Values tuple = new Values();
            tuple.add("0");
            tuple.add(attributeNames);
            tuple.add(attributeValues);
            if (collector != null) {
                if (anchor != null)
                    collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), anchor, tuple);
                else
                    collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), tuple);
//                logger.info("dispatch() primary key is maintained by task " + dispatchInfo.get(dispatchInfo.get(0)) + ".");
            }
            if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                dispatchInfo.set(0, 1);
            }else {
                int tmp = dispatchInfo.get(0);
                dispatchInfo.set(0, ++tmp);
            }
//            logger.info("dispatch() incremented index to task " + dispatchInfo.get(dispatchInfo.get(0)) + ".");
            primaryRelationIndex.put(primaryKey, dispatchInfo);
        }else {
            if (taskToRelationIndex.get(primaryRelationName).size() > 0) {
//                logger.info("dispatch() primary key is not maintained, pick random task from " +
//                        taskToRelationIndex.get(primaryRelationName).size() + " tasks.");
                Integer victimTask = taskToRelationIndex.get(primaryRelationName).get(0);
//                logger.info("dispatch() picked task " + victimTask + " to send tuple to.");
                ArrayList<Integer> sharedKeyTasks = new ArrayList<>();
                Iterator<Map.Entry<String, List<Integer>>> iterator = primaryRelationIndex.entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<String, List<Integer>> pair = iterator.next();
                    if (pair.getValue().lastIndexOf(victimTask) != -1 && pair.getValue().lastIndexOf(victimTask) != 0) {
                        List<Integer> tmp = new ArrayList<>(pair.getValue().subList(1, pair.getValue().size()));
                        tmp.remove(victimTask);
                        tmp.removeAll(sharedKeyTasks);
                        sharedKeyTasks.addAll(tmp);
                    }
                }
                sharedKeyTasks.add(victimTask);
                sharedKeyTasks.add(0, 1);
                Values tuple = new Values();
                tuple.add("0");
                tuple.add(attributeNames);
                tuple.add(attributeValues);
                if (collector != null) {
                    if (anchor != null)
                        collector.emitDirect(victimTask, anchor, tuple);
                    else
                        collector.emitDirect(victimTask, tuple);
                }
                primaryRelationIndex.put(primaryKey, sharedKeyTasks);
//                logger.info("dispatch() shared keys with task " + victimTask + " are tasks: " + sharedKeyTasks.toString());
            }
        }
        /**
         * JOIN: Just retrieve the active tasks that contain tuples with the foreign-key and
         * send the incoming tuple to all of them
         */
        if(secondaryRelationIndex.containsKey(foreignKey)) {
            List<Integer> dispatchInfo = new ArrayList<>(secondaryRelationIndex.get(foreignKey)
                    .subList(1, secondaryRelationIndex.get(foreignKey).size()));
            Values tuple = new Values();
            tuple.add("0");
            tuple.add(attributeNames);
            tuple.add(attributeValues);
            for(Integer task : dispatchInfo) {
                if (collector != null) {
                    if (anchor != null)
                        collector.emitDirect(task, anchor, tuple);
                    else
                        collector.emitDirect(task, tuple);
                }
            }
        }
        return 0;
    }

    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationKey));
            String foreignKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationForeignKey));
            dispatch(primaryKey, foreignKey, outerRelationIndex, outerRelationName, innerRelationIndex,
                    attributeNames, attributeValues, collector, anchor);
        }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationKey));
            String foreignKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationForeignKey));
            dispatch(primaryKey, foreignKey, innerRelationIndex, innerRelationName, outerRelationIndex,
                    attributeNames, attributeValues, collector, anchor);

        }
        return 0;
    }

    public void initializeState(List<Values> state) {
        this.state = state;
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public void mergeState(List<Values> state) {
        HashMap<String, List<Integer>> receivedOuterRelationIndex = (HashMap<String, List<Integer>>) state.get(0).get(0);
        HashMap<String, List<Integer>> receivedInnerRelationIndex = (HashMap<String, List<Integer>>) state.get(0).get(1);
        merge(receivedOuterRelationIndex, outerRelationIndex);
        merge(receivedInnerRelationIndex, innerRelationIndex);
    }

    /**
     * Function to merge the keys from two relation indices
     * @param receivedRelationIndex
     * @param currentRelationIndex
     */
    private void merge(HashMap<String, List<Integer>> receivedRelationIndex, HashMap<String, List<Integer>> currentRelationIndex) {
        Iterator<Map.Entry<String, List<Integer>>> iterator = receivedRelationIndex.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<Integer>> pair = iterator.next();
            if (currentRelationIndex.containsKey(pair.getKey())) {
                List<Integer> receivedList = pair.getValue();
                List<Integer> currentList = currentRelationIndex.get(pair.getKey());
                for (int i = 1; i < receivedList.size(); i++) {
                    if (currentList.lastIndexOf(receivedList.get(i)) < 0) {
                        currentList.add(receivedList.get(i));
                    }
                }
                currentList.set(0, 1);
                currentRelationIndex.put(pair.getKey(), currentList);
            }else {
                List<Integer> receivedList = pair.getValue();
                receivedList.set(0, 1);
                currentRelationIndex.put(pair.getKey(), receivedList);
            }
        }
    }

    public List<Values> getState() {
        state.clear();
        Values values = new Values();
        values.add(outerRelationIndex);
        values.add(innerRelationIndex);
        state.add(values);
        return state;
    }

    /**
     * TODO: Need to complete this
     * @param scaleAction
     * @param taskWithIdentifier
     * @param result
     */
    public void updateIndex(String scaleAction, String taskWithIdentifier, ConcurrentHashMap<String, ArrayList<String>> result) {
        if (scaleAction.equals("add") || scaleAction.equals("activate")) {

        }else if (scaleAction.equals("remove") || scaleAction.equals("deactivate")) {

        }
    }
}
