package gr.katsip.synefo.storm.operators.relational.elastic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingWindowJoin {

	public class BasicWindow {
		
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
		this.circularCacheSize = (int) Math.floor(this.windowSize / slide);
		this.tupleSchema = new Fields(tupleSchema.toList());
		this.joinAttribute = joinAttribute;
	}
	
	public void insertTuple(Long currentTimestamp, Values tuple) {
		if(circularCache.size() > 0 && circularCache.getFirst().startingTimestamp <= currentTimestamp && 
				circularCache.getFirst().endingTimestamp >= currentTimestamp) {
			/**
			 * Insert tuple in the first window
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
			 * Need to evict a basic window (the last one)
			 */
			if(circularCache.size() >= circularCacheSize) {
				circularCache.removeLast();
			}
			BasicWindow basicWindow = new BasicWindow();
			basicWindow.startingTimestamp = currentTimestamp;
			basicWindow.endingTimestamp = currentTimestamp + slide;
			basicWindow.tuples = new HashMap<String, ArrayList<Values>>();
			ArrayList<Values> tupleList = new ArrayList<Values>();
			tupleList.add(tuple);
			basicWindow.tuples.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), tupleList);
		}
	}
	
	public ArrayList<Values> joinTuple(Long currentTimestamp, Values tuple, Fields otherSchema, String otherRelationJoinAttribute) {
		String tupleJoinAttribute = (String) tuple.get(otherSchema.fieldIndex(otherRelationJoinAttribute));
		ArrayList<Values> result = new ArrayList<Values>();
		for(BasicWindow basicWindow : circularCache) {
			if(basicWindow.startingTimestamp <= currentTimestamp && basicWindow.endingTimestamp >= currentTimestamp) {
				if(basicWindow.tuples.containsKey(tupleJoinAttribute)) {
//					result.addAll(new ArrayList<Values>(basicWindow.tuples.get(tupleJoinAttribute)));
					ArrayList<Values> storedTuples = basicWindow.tuples.get(tupleJoinAttribute);
					for(Values t : storedTuples) {
						Values joinTuple = new Values(t.toArray());
						joinTuple.addAll(tuple);
						result.add(joinTuple);
					}
				}
			}else {
				break;
			}
		}
		return result;
	}
}
