package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by katsip on 9/11/2015.
 */
public class FullStateDispatcher implements Serializable, Dispatcher {

    Logger logger = LoggerFactory.getLogger(FullStateDispatcher.class);

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private String outerRelationForeignKey;

    private String innerRelationForeignKey;

    private HashMap<String, List<Integer>> outerRelationIndex;

    private HashMap<String, List<Integer>> innerRelationIndex;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    private Fields outputSchema;

    private long stateSize = 0L;

    public FullStateDispatcher(String outerRelationName, Fields outerRelationSchema,
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
     * This function initializes the internal structure which keeps track of the active tasks and in
     * which relation they belong to.
     * @caution Need to have the ACTIVE tasks!
     * @param taskToRelationIndex the Hash-map with the initially active tasks and the relation they belong to.
     */
    @Override
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex) {
        this.taskToRelationIndex = new HashMap<>(taskToRelationIndex);
    }

    @Override
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
                logger.info("dispatch() primary key is maintained by task " + dispatchInfo.get(dispatchInfo.get(0)) + ".");
            }
            if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                dispatchInfo.set(0, 1);
            }else {
                int tmp = dispatchInfo.get(0);
                dispatchInfo.set(0, ++tmp);
            }
            logger.info("dispatch() incremented index to task " + dispatchInfo.get(dispatchInfo.get(0)) + ".");
            primaryRelationIndex.put(primaryKey, dispatchInfo);
        }else {
            if (taskToRelationIndex.get(primaryRelationName).size() > 0) {
//                logger.info("dispatch() primary key is not maintained, pick random task from " +
//                        taskToRelationIndex.get(primaryRelationName).size() + " tasks.");
                Integer victimTask = taskToRelationIndex.get(primaryRelationName).get(0);
//                logger.info("dispatch() picked task " + victimTask + " to send tuple to.");
                ArrayList<Integer> tasks = new ArrayList<>();
                tasks.add(victimTask);
                tasks.add(0, 1);
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
                //Increment state by the size of shared-key-tasks (bytes) and the length of the key + pointer (int)
                stateSize = stateSize + tasks.toString().length() + primaryKey.length() + 4;
                primaryRelationIndex.put(primaryKey, tasks);
//                logger.info("dispatch() shared keys with task " + victimTask + " are tasks: " + tasks.toString());
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

    @Override
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

    @Override
    public Fields getOutputSchema() {
        return outputSchema;
    }

    @Override
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

    @Override
    public List<Values> getState() {
        List<Values> state = new ArrayList<Values>();
        Values values = new Values();
        values.add(outerRelationIndex);
        values.add(innerRelationIndex);
        state.add(values);
        return state;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    /**
     * @param scaleAction
     * @param taskWithIdentifier
     * @param result
     */
    @Override
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {
        Integer identifier = Integer.parseInt(taskWithIdentifier.split(":")[1]);
        HashMap<String, List<Integer>> relationIndex = null;
        if (relation.equals(innerRelationName))
            relationIndex = innerRelationIndex;
        else if (relation.equals(outerRelationName))
            relationIndex = outerRelationIndex;
        else
            return;
        if (scaleAction.equals("add") || scaleAction.equals("activate")) {
            /**
             * First update the taskToRelationIndex (add the task that was removed)
             */
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.add(identifier);
            taskToRelationIndex.put(relation, tasks);
            for (String key : result) {
                if (relationIndex.containsKey(key)) {
                    List<Integer> currentIndex = relationIndex.get(key);
                    if (currentIndex.lastIndexOf(identifier) <= 0) {
                        currentIndex.add(identifier);
                        currentIndex.set(0, 1);
                        relationIndex.put(key, currentIndex);
                    }
                }else {
                    List<Integer> newIndex = new ArrayList<>();
                    newIndex.add(1);
                    newIndex.add(identifier);
                    relationIndex.put(key, newIndex);
                }
            }
        }else if (scaleAction.equals("remove") || scaleAction.equals("deactivate")) {
            /**
             * First update the taskToRelationIndex (remove the task that was removed)
             */
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.remove(tasks.lastIndexOf(identifier));
            taskToRelationIndex.put(relation, tasks);
            /**
             * Then update for each key the task that received it.
             */
            for (String addedKeysToTask : result) {
                Integer task = Integer.parseInt(addedKeysToTask.split("=")[0]);
                String[] newKeys = addedKeysToTask.split("=")[1].split(",");
                for (String key : newKeys) {
                    if (relationIndex.containsKey(key)) {
                        List<Integer> currentIndex = relationIndex.get(key);
                        if (currentIndex.lastIndexOf(task) <= 0) {
                            currentIndex.add(task);
                            currentIndex.set(0, 1);
                            relationIndex.put(key, currentIndex);
                        }
                    }else {
                        List<Integer> newIndex = new ArrayList<>();
                        newIndex.add(1);
                        newIndex.add(task);
                        relationIndex.put(key, newIndex);
                    }
                }
            }
        }
    }

}
