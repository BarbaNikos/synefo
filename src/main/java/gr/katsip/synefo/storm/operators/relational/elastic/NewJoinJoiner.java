package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.api.Pair;
import org.apache.commons.lang.ArrayUtils;

import java.io.Serializable;
import java.util.*;

/**
 * Created by katsip on 9/21/2015.
 */
public class NewJoinJoiner implements Serializable {

    private String storedRelation;

    private Fields storedRelationSchema;

    private String otherRelation;

    private Fields otherRelationSchema;

    private String storedJoinAttribute;

    private String otherJoinAttribute;

    private List<Values> stateValues;

    private Fields outputSchema;

    private Fields joinOutputSchema;

    private SlidingWindowJoin slidingWindowJoin;

    /**
     * Window size in seconds
     */
    private int windowSize;

    /**
     * Window slide in seconds
     */
    private int slide;

    public NewJoinJoiner(String storedRelation, Fields storedRelationSchema, String otherRelation,
                         Fields otherRelationSchema, String storedJoinAttribute, String otherJoinAttribute, int window, int slide) {
        this.storedRelation = storedRelation;
        this.storedRelationSchema = new Fields(storedRelationSchema.toList());
        this.otherRelation = otherRelation;
        this.otherRelationSchema = new Fields(otherRelationSchema.toList());
        this.storedJoinAttribute = storedJoinAttribute;
        this.otherJoinAttribute = otherJoinAttribute;
        if(this.storedRelationSchema.fieldIndex(this.storedJoinAttribute) < 0) {
            throw new IllegalArgumentException("Not compatible stored-relation schema with the join-attribute for the stored relation");
        }
        if(this.otherRelationSchema.fieldIndex(this.otherJoinAttribute) < 0) {
            throw new IllegalArgumentException("Not compatible other-relation schema with the join-attribute for the other relation");
        }
        String[] storedRelationArray = new String[storedRelationSchema.toList().size()];
        storedRelationArray = storedRelationSchema.toList().toArray(storedRelationArray);
        String[] otherRelationArray = new String[otherRelationSchema.toList().size()];
        otherRelationArray = otherRelationSchema.toList().toArray(otherRelationArray);
        if(this.storedRelation.compareTo(this.otherRelation) <= 0) {
            joinOutputSchema = new Fields((String[]) ArrayUtils.addAll(storedRelationArray, otherRelationArray));
        }else {
            joinOutputSchema = new Fields((String[]) ArrayUtils.addAll(otherRelationArray, storedRelationArray));
        }
        this.windowSize = window;
        this.slide = slide;
        slidingWindowJoin = new SlidingWindowJoin(windowSize, this.slide, storedRelationSchema,
                storedJoinAttribute, storedRelation, otherRelation);
    }

    public void init(List<Values> stateValues) {
        this.stateValues = stateValues;
    }

    public void setStateSchema(Fields stateSchema) {
        //Do nothing
    }

    public void setOutputSchema(Fields output_schema) {
        outputSchema = new Fields(output_schema.toList());
    }

    public Pair<Integer, Integer> execute(Tuple anchor, OutputCollector collector,
                       List<Integer> activeTasks, Integer taskIndex, Fields fields,
                       Values values, Long tupleTimestamp) {
        /**
         * Receive a tuple that: attribute[0] : fields, attribute[1] : values
         */
        Integer numberOfTuplesProduced = 0;
        Long currentTimestamp = System.currentTimeMillis();
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        if(Arrays.equals(attributeNames.toList().toArray(), storedRelationSchema.toList().toArray())) {
            /**
             * Store the new tuple
             */
            slidingWindowJoin.insertTuple(currentTimestamp, attributeValues);
        }else if(Arrays.equals(attributeNames.toList().toArray(), otherRelationSchema.toList().toArray()) &&
                activeTasks != null && activeTasks.size() > 0) {
            /**
             * Attempt to join with stored tuples
             */
            List<Values> joinResult = slidingWindowJoin.joinTuple(currentTimestamp, attributeValues,
                    attributeNames, otherJoinAttribute);
            for(Values result : joinResult) {
                Values tuple = new Values();
                /**
                 * Add timestamp for synefo
                 */
                tuple.add(tupleTimestamp.toString());
                tuple.add(joinOutputSchema);
                tuple.add(result);
                collector.emitDirect(activeTasks.get(taskIndex), anchor, tuple);
                numberOfTuplesProduced++;
                taskIndex += 1;
                if(taskIndex >= activeTasks.size())
                    taskIndex = 0;
            }
        }
        return new Pair<>(taskIndex, numberOfTuplesProduced);
    }

    public List<Values> getStateValues() {
        stateValues.clear();
        Values state = new Values();
        state.add(slidingWindowJoin);
        stateValues.add(state);
        return stateValues;
    }

    public Fields getStateSchema() {
        return null;
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public List<String> setState(HashMap<String, ArrayList<Values>> statePacket) {
        slidingWindowJoin.initializeState(statePacket);
        List<String> keys = new ArrayList<>();
        Iterator<Map.Entry<String, ArrayList<Values>>> iterator = statePacket.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ArrayList<Values>> entry = iterator.next();
            if (keys.lastIndexOf(entry.getKey()) < 0)
                keys.add(entry.getKey());
        }
        return keys;
    }

    public void addToState(HashMap<String, ArrayList<Values>> statePacket) {
        long currentTimestamp = System.currentTimeMillis();
        Iterator<Map.Entry<String, ArrayList<Values>>> iterator = statePacket.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ArrayList<Values>> entry = iterator.next();
            for (Values tuple : entry.getValue()) {
                slidingWindowJoin.insertTuple(currentTimestamp, tuple);
            }
        }
    }

    public HashMap<String, ArrayList<Values>> getStateToBeSent() {
        return this.slidingWindowJoin.getStatePart().tuples;
    }

    public String operatorStep() {
        return "JOIN";
    }

    public String relationStorage() {
        return storedRelation;
    }

    public long getStateSize() {
        return slidingWindowJoin.getStateSize();
    }

    public Fields getJoinOutputSchema() {
        return new Fields(joinOutputSchema.toList());
    }

}
