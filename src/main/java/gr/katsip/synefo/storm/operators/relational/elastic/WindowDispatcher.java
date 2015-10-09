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
                         Fields attributeNames, Values attributeValues, List<Integer> dispatchTable, OutputCollector collector, Tuple anchor) {
        if (primaryRelationIndex.containsKey(primaryKey)) {
            List<Integer> dispatchInfo = primaryRelationIndex.get(primaryKey);
            if (dispatchTable.indexOf(dispatchInfo.get(dispatchInfo.get(0))) >= 0)
                dispatchTable.add(dispatchInfo.get(dispatchInfo.get(0)));
            if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                dispatchInfo.set(0, 1);
            }else {
                int tmp = dispatchInfo.get(0);
                dispatchInfo.set(0, ++tmp);
            }
        }
        /**
         * JOIN part
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
        long currentTimestamp = System.currentTimeMillis();
        Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
        Values attributeValues = (Values) values.get(1);
        List<Integer> dispatchTable = new ArrayList<Integer>();
        /**
         * STORE part
         */
        if(circularCache.size() >= cacheSize && circularCache.getLast().end <= currentTimestamp) {
            DispatchWindow window = circularCache.removeLast();
            stateSize -= window.stateSize;
        }
        for (DispatchWindow window : circularCache) {
            /**
             * STORE (if key has came across before) & JOIN part
             */
            if(window.end >= currentTimestamp) {
                if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
                    String primaryKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationKey));
                    String foreignKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationForeignKey));
                    dispatch(primaryKey, foreignKey, window.outerRelationIndex, outerRelationName, window.innerRelationIndex,
                            attributeNames, attributeValues, dispatchTable, collector, anchor);
                }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
                    String primaryKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationKey));
                    String foreignKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationForeignKey));
                    dispatch(primaryKey, foreignKey, window.innerRelationIndex, innerRelationName, window.outerRelationIndex,
                            attributeNames, attributeValues, dispatchTable, collector, anchor);
                }
            }
        }
        Values tuple = new Values();
        tuple.add("0");
        tuple.add(attributeNames);
        tuple.add(attributeValues);
        for (Integer task : dispatchTable) {
            if (anchor != null)
                collector.emitDirect(task, anchor, tuple);
            else
                collector.emitDirect(task, tuple);
        }
        /**
         * STORE also on the current dispatch window
         */
        if (circularCache.getFirst().end < currentTimestamp) {
            DispatchWindow window = new DispatchWindow();
            if (circularCache.size() > 0)
                window.start = circularCache.getFirst().start + slide;
            else
                window.start = currentTimestamp;
            window.end = window.start + slide;
            circularCache.addFirst(window);
        }
        if (Arrays.equals(attributeNames.toList().toArray(), outerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(outerRelationSchema.fieldIndex(outerRelationKey));
            if (taskToRelationIndex.get(outerRelationName).size() > 0) {
                Integer victimTask = taskToRelationIndex.get(outerRelationName).get(0);
                ArrayList<Integer> tasks = new ArrayList<>();
                tasks.add(victimTask);
                tasks.add(0, 1);
                stateSize = stateSize + tasks.toString().length() + primaryKey.length() + 4;
                circularCache.getFirst().stateSize = circularCache.getFirst().stateSize +
                        tasks.toString().length() + primaryKey.length() + 4;
                circularCache.getFirst().outerRelationIndex.put(primaryKey, tasks);
                if (collector != null && dispatchTable.indexOf(victimTask) < 0) {
                    if (anchor != null)
                        collector.emitDirect(victimTask, anchor, tuple);
                    else
                        collector.emitDirect(victimTask, tuple);
                }
            }
        }else if (Arrays.equals(attributeNames.toList().toArray(), innerRelationSchema.toList().toArray())) {
            String primaryKey = (String) attributeValues.get(innerRelationSchema.fieldIndex(innerRelationKey));
            if (taskToRelationIndex.get(innerRelationName).size() > 0) {
                Integer victimTask = taskToRelationIndex.get(innerRelationName).get(0);
                ArrayList<Integer> tasks = new ArrayList<>();
                tasks.add(victimTask);
                tasks.add(0, 1);
                stateSize = stateSize + tasks.toString().length() + primaryKey.length() + 4;
                circularCache.getFirst().stateSize = circularCache.getFirst().stateSize +
                        tasks.toString().length() + primaryKey.length() + 4;
                circularCache.getFirst().innerRelationIndex.put(primaryKey, tasks);
                if (collector != null && dispatchTable.indexOf(victimTask) < 0) {
                    if (anchor != null)
                        collector.emitDirect(victimTask, anchor, tuple);
                    else
                        collector.emitDirect(victimTask, tuple);
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
        //TODO: Complete this one
        DispatchWindow receivedWindow = (DispatchWindow) state.get(0).get(0);

    }

    @Override
    public List<Values> getState() {
        if (circularCache.size() > 0) {
            Random rand = new Random();
            int index = rand.nextInt(circularCache.size());
            DispatchWindow window = circularCache.remove(index);
            stateSize -= window.stateSize;
            Values tuple = new Values();
            tuple.add(window);
            List<Values> state = new ArrayList<>();
            state.add(tuple);
            return state;
        }else {
            DispatchWindow window = new DispatchWindow();
            stateSize -= window.stateSize;
            Values tuple = new Values();
            tuple.add(window);
            List<Values> state = new ArrayList<>();
            state.add(tuple);
            return state;
        }
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {
        //TODO: Complete this one
    }
}
