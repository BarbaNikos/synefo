package gr.katsip.synefo.storm.operators.relational.elastic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractJoinOperator;

public class JoinJoiner implements AbstractJoinOperator {
	
	private String storedRelation;
	
	private Fields storedRelationSchema;
	
	private String otherRelation;
	
	private Fields otherRelationSchema;
	
	private String joinAttribute;
	
	private List<Values> stateValues;
	
	private Fields outputSchema;

	public JoinJoiner(String storedRelation, Fields storedRelationSchema, String otherRelation, 
			Fields otherRelationSchema, String joinAttribute) {
		this.storedRelation = storedRelation;
		this.storedRelationSchema = new Fields(storedRelationSchema.toList());
		this.otherRelation = otherRelation;
		this.otherRelationSchema = new Fields(otherRelationSchema.toList());
		this.joinAttribute = joinAttribute;
	}
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		//Do nothing
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		outputSchema = new Fields(output_schema.toList());
	}

	@Override
	public void execute(OutputCollector collector,
			HashMap<String, ArrayList<Integer>> taskRelationIndex,
			ArrayList<Integer> activeTasks, Integer taskIndex, Fields fields,
			Values values) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Values> getStateValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getStateSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub

	}

	@Override
	public String operatorStep() {
		return "JOIN";
	}

	@Override
	public String relationStorage() {
		return storedRelation;
	}

}
