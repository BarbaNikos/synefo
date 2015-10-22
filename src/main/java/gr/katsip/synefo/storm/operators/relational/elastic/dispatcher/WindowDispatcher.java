package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.utils.Util;
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

    private long window;

    private long slide;

    private LinkedList<DispatchWindow> ringBuffer;

    private int bufferSize;

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
        ringBuffer = new LinkedList<>();
        this.window = window;
        this.slide = slide;
        bufferSize = (int) Math.ceil(this.window / slide);
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
                         String primaryRelationName, HashMap<String, List<Integer>> secondaryRelationIndex, String secondaryRelationName,
                         Fields attributeNames, Values attributeValues, OutputCollector collector, Tuple anchor) {
        int numberOfTuplesDispatched = 0;
        if(secondaryRelationIndex.containsKey(foreignKey)) {
            List<Integer> dispatchInfo = new ArrayList<>(secondaryRelationIndex.get(foreignKey)
                    .subList(1, secondaryRelationIndex.get(foreignKey).size()));
            Values tuple = new Values();
            tuple.add("0");
            tuple.add(attributeNames);
            tuple.add(attributeValues);
            for(Integer task : dispatchInfo) {
                if (collector != null && taskToRelationIndex.get(secondaryRelationName).contains(task)) {
                    if (anchor != null)
                        collector.emitDirect(task, anchor, tuple);
                    else
                        collector.emitDirect(task, tuple);
                    numberOfTuplesDispatched++;
                }
            }
        }
        return numberOfTuplesDispatched;
    }

    private void cleanup(long currentTimestamp) {
        if(ringBuffer.size() >= bufferSize && (ringBuffer.getLast().start + this.window) <= currentTimestamp) {
            DispatchWindow window = ringBuffer.removeLast();
            stateSize -= window.stateSize;
        }
    }

    private void checkWindow(long currentTimestamp) {
        //Case where the window has progressed by a slide (creation of new current window)
        if (ringBuffer.size() == 0) {
            DispatchWindow window = new DispatchWindow();
            window.start = currentTimestamp;
            window.end = window.start + slide;
            ringBuffer.addFirst(window);
        }else if (ringBuffer.getFirst().end < currentTimestamp && ringBuffer.size() < bufferSize) {
            DispatchWindow window = new DispatchWindow();
            window.start = ringBuffer.getFirst().end + 1;
            window.end = window.start + slide;
            ringBuffer.addFirst(window);
        }
    }

    @Override
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        long currentTimestamp = System.currentTimeMillis();
        int numberOfTuplesDispatched = 0;
        //First check if a window needs to be discarded and add a new window accordingly
        cleanup(currentTimestamp);
        checkWindow(currentTimestamp);

        Values tuple = new Values();
        tuple.add("0");
        tuple.add(fields);
        tuple.add(values);
        /**
         * STORE-and-DISPATCH on the current dispatch window (the first one)
         */
        if (fields.toList().toString().equals(outerRelationSchema.toList().toString())) {
            String primaryKey = (String) values.get(outerRelationSchema.fieldIndex(outerRelationKey));
            if (ringBuffer.getFirst().outerRelationIndex.containsKey(primaryKey)) {
                List<Integer> dispatchInfo = ringBuffer.getFirst().outerRelationIndex.get(primaryKey);
                if (taskToRelationIndex.get(outerRelationName).contains(dispatchInfo.get(dispatchInfo.get(0)))) {
                    if (collector != null) {
                        if (anchor != null)
                            collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), anchor, tuple);
                        else
                            collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), tuple);
                        numberOfTuplesDispatched++;
                    }
                    if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                        dispatchInfo.set(0, 1);
                    } else {
                        int tmp = dispatchInfo.get(0);
                        dispatchInfo.set(0, ++tmp);
                    }
                    ringBuffer.getFirst().outerRelationIndex.put(primaryKey, dispatchInfo);
                }
            }else {
                if (taskToRelationIndex.get(outerRelationName).size() > 0) {
                    Integer victimTask = taskToRelationIndex.get(outerRelationName).get(0);
                    ArrayList<Integer> tasks = new ArrayList<>();
                    tasks.add(1);
                    tasks.add(victimTask);
                    if (collector != null) {
                        if (anchor != null)
                            collector.emitDirect(victimTask, anchor, tuple);
                        else
                            collector.emitDirect(victimTask, tuple);
                        numberOfTuplesDispatched++;
                    }
                    ringBuffer.getFirst().outerRelationIndex.put(primaryKey, tasks);
                    stateSize = stateSize + primaryKey.length() + 4 + 4;
                    ringBuffer.getFirst().stateSize = ringBuffer.getFirst().stateSize + primaryKey.length() + 4 + 4;
                }
            }
        }else if (fields.toList().toString().equals(innerRelationSchema.toList().toString())) {
            String primaryKey = (String) values.get(innerRelationSchema.fieldIndex(innerRelationKey));
            if (ringBuffer.getFirst().innerRelationIndex.containsKey(primaryKey)) {
                List<Integer> dispatchInfo = ringBuffer.getFirst().innerRelationIndex.get(primaryKey);
                if (taskToRelationIndex.get(innerRelationName).contains(dispatchInfo.get(dispatchInfo.get(0)))) {
                    if (collector != null) {
                        if (anchor != null)
                            collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), anchor, tuple);
                        else
                            collector.emitDirect(dispatchInfo.get(dispatchInfo.get(0)), tuple);
                        numberOfTuplesDispatched++;
                    }
                    if (dispatchInfo.get(0) >= (dispatchInfo.size() - 1)) {
                        dispatchInfo.set(0, 1);
                    } else {
                        int tmp = dispatchInfo.get(0);
                        dispatchInfo.set(0, ++tmp);
                    }
                    ringBuffer.getFirst().innerRelationIndex.put(primaryKey, dispatchInfo);
                }
            }else {
                if (taskToRelationIndex.get(innerRelationName).size() > 0) {
                    Integer victimTask = taskToRelationIndex.get(innerRelationName).get(0);
                    ArrayList<Integer> tasks = new ArrayList<>();
                    tasks.add(1);
                    tasks.add(victimTask);
                    if (collector != null) {
                        if (anchor != null)
                            collector.emitDirect(victimTask, anchor, tuple);
                        else
                            collector.emitDirect(victimTask, tuple);
                        numberOfTuplesDispatched++;
                    }
                    ringBuffer.getFirst().innerRelationIndex.put(primaryKey, tasks);
                    stateSize = stateSize + primaryKey.length() + 4 + 4;
                    ringBuffer.getFirst().stateSize = ringBuffer.getFirst().stateSize + primaryKey.length() + 4 + 4;
                }
            }
        }
        /**
         * DISPATCH (if key has came across on a previously-valid windows)
         */
        for (int i = 1; i < ringBuffer.size(); i++) {
            DispatchWindow window = ringBuffer.get(i);
            if((window.start + this.window) > currentTimestamp) {
                if (fields.toList().toString().equals(outerRelationSchema.toList().toString())) {
                    String primaryKey = (String) values.get(outerRelationSchema.fieldIndex(outerRelationKey));
                    String foreignKey = (String) values.get(outerRelationSchema.fieldIndex(outerRelationForeignKey));
                    numberOfTuplesDispatched += dispatch(primaryKey, foreignKey, window.outerRelationIndex, outerRelationName, window.innerRelationIndex, innerRelationName,
                            fields, values, collector, anchor);
                }else if (fields.toList().toString().equals(innerRelationSchema.toList().toString())) {
                    String primaryKey = (String) values.get(innerRelationSchema.fieldIndex(innerRelationKey));
                    String foreignKey = (String) values.get(innerRelationSchema.fieldIndex(innerRelationForeignKey));
                    numberOfTuplesDispatched += dispatch(primaryKey, foreignKey, window.innerRelationIndex, innerRelationName, window.outerRelationIndex, outerRelationName,
                            fields, values, collector, anchor);
                }
            }
        }
        return numberOfTuplesDispatched;
    }

    @Override
    public Fields getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void mergeState(List<Values> state) {
        long currentTimestamp = System.currentTimeMillis();
        DispatchWindow receivedWindow = (DispatchWindow) state.get(0).get(0);
        if (ringBuffer.size() > 0) {
            //Need to integrate current state with the received-window
            //Caution: it will not work if there is a huge gap in CLOCK-DRIFTING
            Util.mergeDispatcherState(ringBuffer.getFirst().innerRelationIndex, receivedWindow.innerRelationIndex);
            Util.mergeDispatcherState(ringBuffer.getFirst().outerRelationIndex, receivedWindow.outerRelationIndex);
            stateSize = stateSize + receivedWindow.stateSize;
            ringBuffer.getFirst().stateSize = ringBuffer.getFirst().stateSize + receivedWindow.stateSize;
        }else {
            receivedWindow.start = currentTimestamp;
            receivedWindow.end = currentTimestamp + slide;
            ringBuffer.addFirst(receivedWindow);
        }
    }

    @Override
    public List<Values> getState() {
        if (ringBuffer.size() > 0) {
            Random rand = new Random();
            int index = rand.nextInt(ringBuffer.size());
            DispatchWindow window = ringBuffer.remove(index);
            stateSize -= window.stateSize;
            Values tuple = new Values();
            tuple.add(window);
            List<Values> state = new ArrayList<>();
            state.add(tuple);
            return state;
        }else {
            DispatchWindow window = new DispatchWindow();
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
        Integer identifier = Integer.parseInt(taskWithIdentifier.split(":")[1]);
        long currentTimestamp = System.currentTimeMillis();
        if (scaleAction.equals("add") || scaleAction.equals("activate")) {
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.add(identifier);
            taskToRelationIndex.put(relation, tasks);
            long additionalStateCounter = 0L;
            if (ringBuffer.size() > 0) {
                /**
                 * Cache is not empty. Records are added to the latest window
                 */
                for (String key : result) {
                    if (relation.equals(innerRelationName)) {
                        if (ringBuffer.getFirst().innerRelationIndex.containsKey(key)) {
                            List<Integer> currentIndex = ringBuffer.getFirst().innerRelationIndex.get(key);
                            if (currentIndex.lastIndexOf(identifier) <= 0) {
                                currentIndex.add(identifier);
                                currentIndex.set(0, 1);
                                ringBuffer.getFirst().innerRelationIndex.put(key, currentIndex);
                                additionalStateCounter += 4;
                            }
                        }else {
                            List<Integer> newIndex = new ArrayList<>();
                            newIndex.add(1);
                            newIndex.add(identifier);
                            additionalStateCounter = additionalStateCounter + key.length() + 4 + 4;
                            ringBuffer.getFirst().innerRelationIndex.put(key, newIndex);
                            ringBuffer.getFirst().stateSize = ringBuffer.getFirst().stateSize + key.length() + 4 + 4;
                        }
                    }
                }
                stateSize += additionalStateCounter;
            }else {
                /**
                 * Cache is empty. New window is created
                 */
                DispatchWindow window = new DispatchWindow();
                window.start = currentTimestamp;
                window.end = currentTimestamp + slide;
                for (String key : result) {
                    if (relation.equals(innerRelationName)) {
                        if (window.innerRelationIndex.containsKey(key)) {
                            List<Integer> currentIndex = window.innerRelationIndex.get(key);
                            if (currentIndex.lastIndexOf(identifier) <= 0) {
                                currentIndex.add(identifier);
                                currentIndex.set(0, 1);
                                window.innerRelationIndex.put(key, currentIndex);
                                additionalStateCounter += 4;
                                window.stateSize += 4;
                            }
                        }else {
                            List<Integer> newIndex = new ArrayList<>();
                            newIndex.add(1);
                            newIndex.add(identifier);
                            window.innerRelationIndex.put(key, newIndex);
                            additionalStateCounter += (4 + 4 + key.length());
                            window.stateSize += (4 + 4 + key.length());
                        }
                    }else if (relation.equals(outerRelationName)) {
                        if (window.outerRelationIndex.containsKey(key)) {
                            List<Integer> currentIndex = window.outerRelationIndex.get(key);
                            if (currentIndex.lastIndexOf(identifier) <= 0) {
                                currentIndex.add(identifier);
                                currentIndex.set(0, 1);
                                window.outerRelationIndex.put(key, currentIndex);
                                additionalStateCounter += 4;
                                window.stateSize += 4;
                            }
                        }else {
                            List<Integer> newIndex = new ArrayList<>();
                            newIndex.add(1);
                            newIndex.add(identifier);
                            window.outerRelationIndex.put(key, newIndex);
                            additionalStateCounter += (4 + 4 + key.length());
                            window.stateSize += (4 + 4 + key.length());
                        }
                    }
                }
                stateSize += additionalStateCounter;
            }
        }else if (scaleAction.equals("remove") || scaleAction.equals("deactivate")) {
            /**
             * First update the taskToRelationIndex (remove the task that was removed)
             */
            List<Integer> tasks = taskToRelationIndex.get(relation);
            tasks.remove(tasks.lastIndexOf(identifier));
            taskToRelationIndex.put(relation, tasks);
            if (ringBuffer.size() > 0) {
                for (String addedKeysToTask : result) {
                    Integer task = Integer.parseInt(addedKeysToTask.split("=")[0]);
                    String[] newKeys = addedKeysToTask.split("=")[1].split(",");
                    for (String key : newKeys) {
                        if (relation.equals(innerRelationName)) {
                            if (ringBuffer.getFirst().innerRelationIndex.containsKey(key)) {
                                List<Integer> currentIndex = ringBuffer.getFirst().innerRelationIndex.get(key);
                                if (currentIndex.lastIndexOf(task) <= 0) {
                                    currentIndex.add(task);
                                    currentIndex.set(0, 1);
                                    ringBuffer.getFirst().innerRelationIndex.put(key, currentIndex);
                                }
                            }else {
                                List<Integer> newIndex = new ArrayList<>();
                                newIndex.add(1);
                                newIndex.add(task);
                                ringBuffer.getFirst().innerRelationIndex.put(key, newIndex);
                            }
                        }else if (relation.equals(outerRelationName)) {
                            if (ringBuffer.getFirst().outerRelationIndex.containsKey(key)) {
                                List<Integer> currentIndex = ringBuffer.getFirst().outerRelationIndex.get(key);
                                if (currentIndex.lastIndexOf(task) <= 0) {
                                    currentIndex.add(task);
                                    currentIndex.set(0, 1);
                                    ringBuffer.getFirst().outerRelationIndex.put(key, currentIndex);
                                }
                            }else {
                                List<Integer> newIndex = new ArrayList<>();
                                newIndex.add(1);
                                newIndex.add(task);
                                ringBuffer.getFirst().outerRelationIndex.put(key, newIndex);
                            }
                        }
                    }
                }
            }else {
                DispatchWindow window = new DispatchWindow();
                window.start = currentTimestamp;
                window.end = currentTimestamp + slide;
                for (String addedKeysToTask : result) {
                    Integer task = Integer.parseInt(addedKeysToTask.split("=")[0]);
                    String[] newKeys = addedKeysToTask.split("=")[1].split(",");
                    for (String key : newKeys) {
                        if (relation.equals(innerRelationName)) {
                            if (window.innerRelationIndex.containsKey(key)) {
                                List<Integer> currentIndex = window.innerRelationIndex.get(key);
                                if (currentIndex.lastIndexOf(task) <= 0) {
                                    currentIndex.add(task);
                                    currentIndex.set(0, 1);
                                    window.innerRelationIndex.put(key, currentIndex);
                                }
                            }else {
                                List<Integer> newIndex = new ArrayList<>();
                                newIndex.add(1);
                                newIndex.add(task);
                                window.innerRelationIndex.put(key, newIndex);
                            }
                        }else if (relation.equals(outerRelationName)) {
                            if (window.outerRelationIndex.containsKey(key)) {
                                List<Integer> currentIndex = window.outerRelationIndex.get(key);
                                if (currentIndex.lastIndexOf(task) <= 0) {
                                    currentIndex.add(task);
                                    currentIndex.set(0, 1);
                                    window.outerRelationIndex.put(key, currentIndex);
                                }
                            }else {
                                List<Integer> newIndex = new ArrayList<>();
                                newIndex.add(1);
                                newIndex.add(task);
                                window.outerRelationIndex.put(key, newIndex);
                            }
                        }
                    }
                }
                ringBuffer.addFirst(window);
            }
        }
    }
}
