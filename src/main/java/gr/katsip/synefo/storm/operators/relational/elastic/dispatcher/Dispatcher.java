package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;

/**
 * Created by katsip on 10/8/2015.
 */
public interface Dispatcher {
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex);
    public void setOutputSchema(Fields outputSchema);
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values);
    public Fields getOutputSchema();
    public void mergeState(List<Values> state);
    public List<Values> getState();
    public long getStateSize();
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result);
}
