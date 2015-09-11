package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by katsip on 9/11/2015.
 */
public class NewJoinDispatcher {

    private Fields outputSchema;

    private List<Values> stateValues;

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

    private List<Integer> activeTasks = null;

    public NewJoinDispatcher(String outerRelationName, Fields outerRelationSchema,
                             String outerRelationKey, String outerRelationForeignKey,
                             String innerRelationName, Fields innerRelationSchema,
                             String innerRelationKey, String innerRelationForeignKey) {
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
        activeTasks = null;
    }

    /**
     * add task to relation indices inside the Dispatcher's structures.
     * @param relationName the relation to which the operator is added
     * @param originalTask the originally active task
     * @param newTask the newly added operator
     */
    public void expandTask(String relationName, Integer originalTask, Integer newTask) {
        Iterator<Map.Entry<String, List<Integer>>> iterator = null;
        if (relationName.equals(outerRelationName)) {
           iterator = outerRelationIndex.entrySet().iterator();
        }else {
            iterator = innerRelationIndex.entrySet().iterator();
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
                break;
            }
        }
        if (relationName.equals(outerRelationName)) {
            outerRelationIndex.put(key, taskList);
        }else {
            innerRelationIndex.put(key, taskList);
        }
        List<Integer> tasks = taskToRelationIndex.get(relationName);
        tasks.add(newTask);
        taskToRelationIndex.put(relationName, tasks);
    }

    /**
     * remove task from relation indices inside the Dispatcher's structures.
     * @param relationName the relation from which the operator belongs to
     * @param remainingTask the remaining active operator
     * @param removedTask the removed operator
     */
    public void mergeTask(String relationName, Integer remainingTask, Integer removedTask) {
        Iterator<Map.Entry<String, List<Integer>>> iterator = null;
        if (relationName.equals(outerRelationName)) {
            iterator = outerRelationIndex.entrySet().iterator();
        }else {
            iterator = innerRelationIndex.entrySet().iterator();
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
                break;
            }
        }
        if (relationName.equals(outerRelationName)) {
            outerRelationIndex.put(key, taskList);
        }else {
            innerRelationIndex.put(key, taskList);
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

    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values, Long tupleTimestamp) {
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationKey));
            String foreignKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationForeignKey));
            /**
             * STORE:
             * 2 cases: (a) primary-key has been encountered before (b) primary-key has not been encountered before
             * case (a): Send it to the next available operator
             * case (b): Pick one of the active nodes randomly and send the key there.
             *              In case of (b), all the other nodes
             *              that share common keys with the selected node, will receive same keys also
             */
            if (outerRelationIndex.containsKey(primaryKey)) {
                List<Integer> dispatchInfo = outerRelationIndex.get(primaryKey);
                Values tuple = new Values();
                tuple.add(tupleTimestamp.toString());
                tuple.add(attributeNames);
                tuple.add(attributeValues);
                collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), anchor, tuple);
                if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                    dispatchInfo.set(0, 1);
                }else {
                    int tmp = dispatchInfo.get(0);
                    dispatchInfo.set(0, ++tmp);
                }
            }else {
                Random rnd = new Random();
                Integer randomTask = taskToRelationIndex.get(outerRelationName)
                        .get(rnd.nextInt(taskToRelationIndex.get(outerRelationName).size()));
                ArrayList<Integer> sharedKeyTasks = new ArrayList<>();
                Iterator<Map.Entry<String, List<Integer>>> iterator = outerRelationIndex.entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<String, List<Integer>> pair = iterator.next();
                    if (pair.getValue().lastIndexOf(randomTask) != -1 && pair.getValue().lastIndexOf(randomTask) != 0) {
                        List<Integer> tmp = new ArrayList<>(pair.getValue().subList(1, pair.getValue().size()));
                        tmp.remove(randomTask);
                        tmp.retainAll(sharedKeyTasks);
                        sharedKeyTasks.addAll(tmp);
                    }
                }
                sharedKeyTasks.add(randomTask);
                sharedKeyTasks.add(0, 1);
                outerRelationIndex.put(primaryKey, sharedKeyTasks);
            }
            /**
             * JOIN: Just retrieve the active tasks that contain tuples with the foreign-key and
             * send the incoming tuple to all of them
             */
            List<Integer> dispatchInfo = new ArrayList<>(innerRelationIndex.get(foreignKey)
                    .subList(1, innerRelationIndex.get(foreignKey).size()));
            Values tuple = new Values();
            tuple.add(tupleTimestamp.toString());
            tuple.add(attributeNames);
            tuple.add(attributeValues);
            for(Integer task : dispatchInfo) {
                collector.emitDirect(task, anchor, tuple);
            }
        }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationKey));
            String foreignKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationForeignKey));
            /**
             * STORE:
             */

            /**
             * JOIN:
             */
        }
    }
}
