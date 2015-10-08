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
 * Created by katsip on 10/8/2015.
 */
public class StatelessDispatcher implements Serializable, Dispatcher {

    Logger logger = LoggerFactory.getLogger(StatelessDispatcher.class);

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private Integer outerRelationIndex;

    private Integer innerRelationIndex;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    private Fields outputSchema;

    private long stateSize = 0L;

    public StatelessDispatcher(String outerRelationName, Fields outerRelationSchema,
                               String outerRelationKey, String outerRelationForeignKey,
                               String innerRelationName, Fields innerRelationSchema,
                               String innerRelationKey, String innerRelationForeignKey, Fields outputSchema) {
        this.outerRelationName = outerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.innerRelationName = innerRelationName;
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        taskToRelationIndex = null;
        this.outputSchema = new Fields(outputSchema.toList());
        this.outerRelationIndex = 0;
        this.innerRelationIndex = 0;
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

    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        Values tuple = new Values();
        tuple.add("0");
        tuple.add(attributeNames);
        tuple.add(attributeValues);
        if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
            /**
             * STORE: Send tuple to one of the active tasks of the outer relation (also increment index)
             */
            List<Integer> activeTaskIdentifiers = taskToRelationIndex.get(outerRelationName);
            collector.emitDirect(activeTaskIdentifiers.get(outerRelationIndex), anchor, tuple);
            if (outerRelationIndex >= (activeTaskIdentifiers.size() - 1))
                outerRelationIndex = 0;
            else
                outerRelationIndex++;
            /**
             * JOIN: Broadcast tuple to all of the active tasks of the inner relation
             */
            activeTaskIdentifiers = taskToRelationIndex.get(innerRelationName);
            for (Integer task : activeTaskIdentifiers) {
                collector.emitDirect(task, anchor, tuple);
            }
        }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
            /**
             * STORE: Send tuple to one of the active tasks of the outer relation (also increment index)
             */
            List<Integer> activeTaskIdentifiers = taskToRelationIndex.get(innerRelationName);
            collector.emitDirect(activeTaskIdentifiers.get(innerRelationIndex), anchor, tuple);
            if (innerRelationIndex >= (activeTaskIdentifiers.size() - 1))
                innerRelationIndex = 0;
            else
                innerRelationIndex++;
            /**
             * JOIN: Broadcast tuple to all of the active tasks of the inner relation
             */
            activeTaskIdentifiers = taskToRelationIndex.get(outerRelationName);
            for (Integer task : activeTaskIdentifiers) {
                collector.emitDirect(task, anchor, tuple);
            }
        }
        return 0;
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public void mergeState(List<Values> state) {

    }

    public List<Values> getState() {
        List<Values> state = new ArrayList<Values>();
        return state;
    }

    public long getStateSize() {
        return stateSize;
    }

    /**
     * @param scaleAction
     * @param taskWithIdentifier
     * @param result
     */
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {
        Integer identifier = Integer.parseInt(taskWithIdentifier.split(":")[1]);
        if (relation.equals(innerRelationName))
            innerRelationIndex = 0;
        else if (relation.equals(outerRelationName))
            outerRelationIndex = 0;
        else
            return;
        if (scaleAction.equals("add") || scaleAction.equals("activate")) {
            /**
             * First update the taskToRelationIndex (add the task that was removed)
             */
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.add(identifier);
            taskToRelationIndex.put(relation, tasks);
        }else if (scaleAction.equals("remove") || scaleAction.equals("deactivate")) {
            /**
             * First update the taskToRelationIndex (remove the task that was removed)
             */
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.remove(tasks.lastIndexOf(identifier));
            taskToRelationIndex.put(relation, tasks);
        }
    }

}
