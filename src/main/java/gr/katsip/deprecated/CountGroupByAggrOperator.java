package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public class CountGroupByAggrOperator implements Serializable, AbstractOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -564093865953906202L;

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields outputSchema;

	private String[] groupByAttributes;

	private Integer window;

	public CountGroupByAggrOperator(int window, String[] groupByAttributes) {
		this.groupByAttributes = groupByAttributes;
		this.window = window;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> returnValues = new ArrayList<Values>();
		StringBuilder strBuild = new StringBuilder();
		for(int i = 0; i < groupByAttributes.length; i++) {
			strBuild.append(values.get(fields.fieldIndex(groupByAttributes[i])));
			strBuild.append(',');
		}
		strBuild.setLength(Math.max(strBuild.length() - 1, 0));
		
		String groupByAttrs = strBuild.toString();
		Integer count = -1;
		for(int i = 0; i < stateValues.size(); i++) {
			if(groupByAttrs.equalsIgnoreCase((String) stateValues.get(i).get(0))) {
				count = (Integer) stateValues.get(i).get(1);
				count += 1;
				Values v = stateValues.get(i);
				v.set(1, count);
				stateValues.set(i, v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
				break;
			}
		}
		if(count == -1) {
			if(stateValues.size() >= window) {
				while(stateValues.size() > window) {
					/**
					 * Locate the tuple with the earliest time 
					 * index (it is the last value added in the end of a tuple)
					 */
					long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
					int idx = 0;
					for(int i = 0; i < stateValues.size(); i++) {
						if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
							earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
							idx = i;
						}
					}
					stateValues.remove(idx);
				}
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}else {
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}
		}
		return returnValues;
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		stateValues.addAll(receivedStateValues);
		if(stateValues.size() <= window) {
			return;
		}else {
			while(stateValues.size() > window) {
				/**
				 * Locate the tuple with the earliest time 
				 * index (it is the last value added in the end of a tuple)
				 */
				long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
				int idx = 0;
				for(int i = 0; i < stateValues.size(); i++) {
					if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
						earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
						idx = i;
					}
				}
				stateValues.remove(idx);
			}
		}
	}

	@Override
	public void setOutputSchema(Fields outputSchema) {
		this.outputSchema = new Fields(outputSchema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
