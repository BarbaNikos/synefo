package gr.katsip.synefo.storm.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface AbstractJoinOperator {

	public void init(List<Values> stateValues);

	public void setStateSchema(Fields stateSchema);

	public void setOutputSchema(Fields output_schema);

	public int execute(OutputCollector collector, HashMap<String, ArrayList<Integer>> taskRelationIndex, 
			ArrayList<Integer> activeTasks, Integer taskIndex, Fields fields, Values values, Long tupleTimestamp);

	public List<Values> getStateValues();

	public Fields getStateSchema();

	public Fields getOutputSchema();

	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues);
	
	public String operatorStep();
	
	public String relationStorage();
	
	public long getStateSize();
	
	public Fields getJoinOutputSchema();
}
