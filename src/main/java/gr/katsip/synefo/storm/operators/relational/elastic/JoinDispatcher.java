package gr.katsip.synefo.storm.operators.relational.elastic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractJoinOperator;

public class JoinDispatcher implements AbstractJoinOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2856458300296445939L;

	List<Values> stateValues;
	
	private String leftRelation;
	
	private String rightRelation;
	
	private Fields leftRelationSchema;
	
	private Fields rightRelationSchema;
	
	private Fields outputSchema;

	public JoinDispatcher(String leftRelation, Fields leftRelationSchema, String rightRelation, 
			Fields rightRelationSchema, Fields outputSchema) {
		this.leftRelation = leftRelation;
		this.rightRelation = rightRelation;
		this.leftRelationSchema = new Fields(leftRelationSchema.toList());
		this.rightRelationSchema = new Fields(rightRelationSchema.toList());
		this.outputSchema = new Fields(outputSchema.toList());
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		//Nothing to do at this point
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		this.outputSchema = new Fields(output_schema.toList());
	}

	@Override
	public void execute(OutputCollector collector,
			HashMap<String, ArrayList<Integer>> taskRelationIndex,
			ArrayList<Integer> activeTasks, Integer taskIndex, Fields fields,
			Values values) {
		/**
		 * Receive a tuple that: attribute[0] : fields, attribute[1] : values
		 */
		Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
		Values attributeValues = (Values) values.get(1);
		if(Arrays.equals(attributeNames.toList().toArray(), leftRelationSchema.toList().toArray())) {
			/**
			 * STORE:
			 * Send it to one active left-relation-storage operators
			 * After giving it an additional SYNEFO_HEADER field
			 */
			ArrayList<Integer> leftRelationTasks = taskRelationIndex.get(leftRelation);
			while(true) {
				Integer nextTask = activeTasks.get(taskIndex);
				if(leftRelationTasks.contains(nextTask)) {
					Values tuple = new Values();
					tuple.add((new Long(System.currentTimeMillis())).toString());
					tuple.add(attributeNames);
					tuple.add(attributeValues);
					collector.emitDirect(nextTask, tuple);
					if(nextTask >= activeTasks.size())
						nextTask = 0;
					else
						nextTask += 1;
					break;
				}
				if(nextTask >= activeTasks.size())
					nextTask = 0;
				else
					nextTask += 1;
			}
			/**
			 * JOIN:
			 * Send it to all active right-relation join operators
			 * After giving it an additional SYNEFO_HEADER field
			 */
			Values tuple = new Values();
			tuple.add((new Long(System.currentTimeMillis())).toString());
			tuple.add(attributeNames);
			tuple.add(attributeValues);
			for(Integer rightRelationTask : taskRelationIndex.get(rightRelation)) {
				if(activeTasks.contains(rightRelationTask)) {
					collector.emitDirect(rightRelationTask, tuple);
				}
			}
		}else if(Arrays.equals(attributeNames.toList().toArray(), rightRelationSchema.toList().toArray())) {
			/**
			 * STORE:
			 * Send it to one active right-relation-storage operators
			 * After giving it an additional SYNEFO_HEADER field
			 */
			ArrayList<Integer> rightRelationTasks = taskRelationIndex.get(rightRelation);
			while(true) {
				Integer nextTask = activeTasks.get(taskIndex);
				if(rightRelationTasks.contains(nextTask)) {
					Values tuple = new Values();
					tuple.add((new Long(System.currentTimeMillis())).toString());
					tuple.add(attributeNames);
					tuple.add(attributeValues);
					collector.emitDirect(nextTask, tuple);
					if(nextTask >= activeTasks.size())
						nextTask = 0;
					else
						nextTask += 1;
					break;
				}
				if(nextTask >= activeTasks.size())
					nextTask = 0;
				else
					nextTask += 1;
			}
			/**
			 * JOIN:
			 * Send it to all active left-relation join operators
			 * After giving it an additional SYNEFO_HEADER field
			 */
			Values tuple = new Values();
			tuple.add((new Long(System.currentTimeMillis())).toString());
			tuple.add(attributeNames);
			tuple.add(attributeValues);
			for(Integer leftRelationTask : taskRelationIndex.get(leftRelation)) {
				if(activeTasks.contains(leftRelationTask)) {
					collector.emitDirect(leftRelationTask, tuple);
				}
			}
		}
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		//Nothing to do here
	}

	@Override
	public String operatorStep() {
		return "DISPATCH";
	}

	@Override
	public String relationStorage() {
		return "NA";
	}

}
