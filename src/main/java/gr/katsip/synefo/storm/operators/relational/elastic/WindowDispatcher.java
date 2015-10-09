package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by katsip on 10/8/2015.
 */
public class WindowDispatcher implements Serializable, Dispatcher {

    Logger logger = LoggerFactory.getLogger(WindowDispatcher.class);

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private String outerRelationForeignKey;

    private String innerRelationForeignKey;

    private Fields outputSchema;

    private long stateSize = 0L;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    public class DispatchWindow implements Serializable {

        public long start;

        public long end;

        public long stateSize = 0L;

        private HashMap<String, List<Integer>> outerRelationIndex;

        private HashMap<String, List<Integer>> innerRelationIndex;

        public DispatchWindow() {
            stateSize = 0L;
            start = 0L;
            end = 0L;
            outerRelationIndex = new HashMap<>();
            innerRelationIndex = new HashMap<>();
        }
    }

    private long window;

    private long slide;

    private LinkedList<DispatchWindow> circularCache;

    private int cacheSize;

    public WindowDispatcher(String outerRelationName, Fields outerRelationSchema,
                            String outerRelationKey, String outerRelationForeignKey,
                            String innerRelationName, Fields innerRelationSchema,
                            String innerRelationKey, String innerRelationForeignKey, Fields outputSchema,
                            long window, long slide) {
        this.outerRelationName = outerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.outerRelationKey = outerRelationKey;
        this.outerRelationForeignKey = outerRelationForeignKey;
        this.innerRelationName = innerRelationName;
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.innerRelationForeignKey = innerRelationForeignKey;
        taskToRelationIndex = null;
        this.outputSchema = new Fields(outputSchema.toList());
        circularCache = new LinkedList<>();
        this.window = window;
        this.slide = slide;
        cacheSize = (int) Math.ceil(window / slide);
    }

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
        if (primaryRelationIndex.containsKey(primaryKey)) {
            List<Integer> dispatchInfo = primaryRelationIndex.get(primaryKey);
            Values tuple = new Values();
            tuple.add("0");
            tuple.add(attributeNames);
            tuple.add(attributeValues);
            if (collector != null) {
                if (anchor != null) {
                    collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), anchor, tuple);
                }else {
                    collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), tuple);
                }
            }
            if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                dispatchInfo.set(0, 1);
            }else {
                int tmp = dispatchInfo.get(0);
                dispatchInfo.set(0, ++tmp);
            }
        }else {

        }
        return 0;
    }

    @Override
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        long currentTimestamp = System.currentTimeMillis();
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        for (DispatchWindow window : circularCache) {
            if(window.end >= currentTimestamp) {
                if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
                    String primaryKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationKey));
                    String foreignKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationForeignKey));
                    dispatch(primaryKey, foreignKey, window.outerRelationIndex, outerRelationName, window.innerRelationIndex,
                            attributeNames, attributeValues, collector, anchor);
                }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
                    String primaryKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationKey));
                    String foreignKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationForeignKey));
                    dispatch(primaryKey, foreignKey, window.innerRelationIndex, innerRelationName, window.outerRelationIndex,
                            attributeNames, attributeValues, collector, anchor);
                }
            }
        }
        return 0;
    }

    @Override
    public Fields getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void mergeState(List<Values> state) {

    }

    @Override
    public List<Values> getState() {
        return null;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {

    }
}
