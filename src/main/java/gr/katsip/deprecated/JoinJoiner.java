package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Tuple;
import gr.katsip.synefo.storm.operators.joiner.WindowEquiJoin;
import org.apache.commons.lang.ArrayUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public class JoinJoiner implements AbstractJoinOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1309799850817632049L;

	private String storedRelation;
	
	private Fields storedRelationSchema;
	
	private String otherRelation;
	
	private Fields otherRelationSchema;
	
	private String storedJoinAttribute;
	
	private String otherJoinAttribute;
	
	private List<Values> stateValues;
	
	private Fields outputSchema;
	
	private Fields joinOutputSchema;
	
	private WindowEquiJoin windowEquiJoin;
	
	/**
	 * Window size in seconds
	 */
	private int windowSize;
	
	/**
	 * Window slide in seconds
	 */
	private int slide;

	public JoinJoiner(String storedRelation, Fields storedRelationSchema, String otherRelation, 
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
		windowEquiJoin = new WindowEquiJoin(windowSize, this.slide, storedRelationSchema,
				storedJoinAttribute, storedRelation, otherRelation);
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
	public int execute(Tuple anchor, OutputCollector collector,
			HashMap<String, ArrayList<Integer>> taskRelationIndex,
			ArrayList<Integer> activeTasks, Integer taskIndex, Fields fields,
			Values values, Long tupleTimestamp) {
		/**
		 * Receive a tuple that: attribute[0] : fields, attribute[1] : values
		 */
		Long currentTimestamp = System.currentTimeMillis();
		Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
		Values attributeValues = (Values) values.get(1);
		if(Arrays.equals(attributeNames.toList().toArray(), storedRelationSchema.toList().toArray())) {
			/**
			 * Store the new tuple
			 */
			windowEquiJoin.insertTuple(currentTimestamp, attributeValues);
		}else if(Arrays.equals(attributeNames.toList().toArray(), otherRelationSchema.toList().toArray()) && 
				activeTasks != null && activeTasks.size() > 0) {
			/**
			 * Attempt to join with stored tuples
			 */
			List<Values> joinResult = windowEquiJoin.joinTuple(currentTimestamp, attributeValues,
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
				taskIndex += 1;
				if(taskIndex >= activeTasks.size())
					taskIndex = 0;
			}
		}
		return taskIndex;
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Values state = new Values();
		state.add(windowEquiJoin);
		stateValues.add(state);
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
	
	@Override
	public long getStateSize() {
		return windowEquiJoin.getStateSize();
	}

	@Override
	public Fields getJoinOutputSchema() {
		return new Fields(joinOutputSchema.toList());
	}

}
