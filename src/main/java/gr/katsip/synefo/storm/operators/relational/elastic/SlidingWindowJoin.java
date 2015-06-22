package gr.katsip.synefo.storm.operators.relational.elastic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingWindowJoin implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -557386415760782256L;

	public class BasicWindow implements Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 687583325823717593L;

		public Long startingTimestamp;
		
		public Long endingTimestamp;
		
		public HashMap<String, ArrayList<Values>> tuples;
		
	}
	
	private int windowSize;
	
	private int slide;
	
	private LinkedList<BasicWindow> circularCache;
	
	private int circularCacheSize;
	
	private Fields tupleSchema;
	
	private String joinAttribute;
	
	/**
	 * 
	 * @param windowSize size of the window in milliseconds
	 * @param slide size of slide in milliseconds
	 * @param tupleSchema
	 * @param joinAttribute
	 */
	public SlidingWindowJoin(int windowSize, int slide, Fields tupleSchema, String joinAttribute) {
		circularCache = new LinkedList<BasicWindow>();
		this.windowSize = windowSize;
		this.slide = slide;
		this.circularCacheSize = (int) (this.windowSize / slide);
		this.tupleSchema = new Fields(tupleSchema.toList());
		this.joinAttribute = joinAttribute;
	}
	
	public void insertTuple(Long currentTimestamp, Values tuple) {
		if(circularCache.size() > 0 && circularCache.getFirst().startingTimestamp <= currentTimestamp && 
				circularCache.getFirst().endingTimestamp >= currentTimestamp) {
			/**
			 * Insert tuple in the first window, if the window is still valid
			 */
			ArrayList<Values> tupleList = null;
			if(circularCache.getFirst().tuples.containsKey(tuple.get(tupleSchema.fieldIndex(joinAttribute)))) {
				tupleList = circularCache.getFirst().tuples.get(tuple.get(tupleSchema.fieldIndex(joinAttribute)));
			}else {
				tupleList = new ArrayList<Values>();
			}
			tupleList.add(tuple);
			circularCache.getFirst().tuples.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), tupleList);
		}else {
			/**
			 * Need to evict a basic window (the last one), if we have used up all basic window slots
			 */
			if(circularCache.size() >= circularCacheSize) {
				circularCache.removeLast();
			}
			/**
			 * Creation of the new basic window
			 */
			BasicWindow basicWindow = new BasicWindow();
			basicWindow.startingTimestamp = currentTimestamp;
			basicWindow.endingTimestamp = currentTimestamp + slide;
			basicWindow.tuples = new HashMap<String, ArrayList<Values>>();
			ArrayList<Values> tupleList = new ArrayList<Values>();
			tupleList.add(tuple);
			basicWindow.tuples.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), tupleList);
			circularCache.add(basicWindow);
		}
	}
	
	/**
	 * method for attempting to join a tuple of the different relation
	 * @param currentTimestamp the given timestamp of arrival for the tuple
	 * @param tuple the actual tuple values
	 * @param otherSchema the schema of the provided tuple
	 * @param otherRelationJoinAttribute the name of the attribute in the given tuple
	 * @return a list with the resulting tuples
	 */
	public ArrayList<Values> joinTuple(Long currentTimestamp, Values tuple, Fields otherSchema, String otherRelationJoinAttribute) {
		String tupleJoinAttribute = (String) tuple.get(otherSchema.fieldIndex(otherRelationJoinAttribute));
		ArrayList<Values> result = new ArrayList<Values>();
		/**
		 * Iterate over all valid windows according to the given timestamp
		 */
		for(BasicWindow basicWindow : circularCache) {
			if(basicWindow.startingTimestamp <= currentTimestamp && basicWindow.endingTimestamp >= currentTimestamp) {
				if(basicWindow.tuples.containsKey(tupleJoinAttribute)) {
					ArrayList<Values> storedTuples = basicWindow.tuples.get(tupleJoinAttribute);
					for(Values t : storedTuples) {
						Values joinTuple = new Values(t.toArray());
						joinTuple.addAll(tuple);
						result.add(joinTuple);
					}
				}
			}else {
				/**
				 * If we overcome the current window, the loop breaks and the result 
				 * can be returned
				 */
				break;
			}
		}
		return result;
	}
	
	public LinkedList<BasicWindow> getCircularCache() {
		return circularCache;
	}
}
