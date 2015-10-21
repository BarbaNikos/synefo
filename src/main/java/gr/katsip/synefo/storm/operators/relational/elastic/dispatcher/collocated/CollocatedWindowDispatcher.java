package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by katsip on 10/21/2015.
 */
public class CollocatedWindowDispatcher implements Serializable, Dispatcher {

    Logger logger = LoggerFactory.getLogger(CollocatedWindowDispatcher.class);

    private long window;

    private long slide;

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private Fields outputSchema;

    private LinkedList<CollocatedDispatchWindow> ringBuffer;

    private int bufferSize;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    public CollocatedWindowDispatcher(String outerRelationName, Fields outerRelationSchema, String outerRelationKey,
                                      String innerRelationName, Fields innerRelationSchema, String innerRelationKey,
                                      Fields outputSchema, long window, long slide) {
        this.window = window;
        this.slide = slide;
        bufferSize = (int) Math.ceil(this.window / this.slide);
        ringBuffer = new LinkedList<>();
        this.outerRelationName = outerRelationName;
        this.innerRelationName = innerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.outerRelationKey = outerRelationKey;
        this.outputSchema = new Fields(outputSchema.toList());
    }

    @Override
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex) {
        this.taskToRelationIndex = new HashMap<>(taskToRelationIndex);
    }

    @Override
    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    @Override
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
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
        return 0;
    }

    @Override
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {

    }
}
