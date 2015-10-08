package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Created by katsip on 10/8/2015.
 */
public class WindowDispatcher implements Serializable, Dispatcher {
    @Override
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex) {

    }

    @Override
    public void setOutputSchema(Fields outputSchema) {

    }

    @Override
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        return 0;
    }

    @Override
    public Fields getOutputSchema() {
        return null;
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
