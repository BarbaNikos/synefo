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
		
		public long basicWindowStateSize;
		
		public long numberOfTuples;
		
	}
	
	private long windowSize;
	
	private long slide;
	
	private LinkedList<BasicWindow> circularCache;
	
	private int circularCacheSize;
	
	private Fields tupleSchema;
	
	private String joinAttribute;
	
	private long stateByteSize;
	
	private long totalNumberOfTuples;
	
	/**
	 * 
	 * @param windowSize size of the window in milliseconds
	 * @param slide size of slide in milliseconds
	 * @param tupleSchema the schema of the tuples that will be stored
	 * @param joinAttribute the name of the join attribute
	 */
	public SlidingWindowJoin(long windowSize, long slide, Fields tupleSchema, String joinAttribute) {
		circularCache = new LinkedList<BasicWindow>();
		this.windowSize = windowSize;
		this.slide = slide;
		this.circularCacheSize = (int) (this.windowSize / slide);
		this.tupleSchema = new Fields(tupleSchema.toList());
		this.joinAttribute = joinAttribute;
		this.stateByteSize = 0L;
		this.totalNumberOfTuples = 0L;
	}
	
	/**
	 * This method adds a given tuple to the store
	 * @param currentTimestamp the timestamp of the insert operation in milliseconds
	 * @param tuple the about-to-be-inserted tuple
	 */
	public void insertTuple(Long currentTimestamp, Values tuple) {
		if(circularCache.size() > 0 && circularCache.getFirst().startingTimestamp <= currentTimestamp && (circularCache.getFirst().startingTimestamp + slide) > currentTimestamp && 
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
			circularCache.getFirst().basicWindowStateSize += tuple.toArray().toString().length();
			circularCache.getFirst().tuples.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), tupleList);
			circularCache.getFirst().numberOfTuples += 1;
			totalNumberOfTuples += 1;
			stateByteSize += tuple.toArray().toString().length();
		}else {
			/**
			 * Need to evict a basic-window (the last one), if we have used up all basic window slots 
			 * and the last basic-window has expired.
			 */
			if(circularCache.size() >= circularCacheSize && circularCache.getLast().endingTimestamp <= currentTimestamp) {
				BasicWindow basicWindow = circularCache.removeLast();
				stateByteSize -= basicWindow.basicWindowStateSize;
				totalNumberOfTuples -= basicWindow.numberOfTuples;
			}
			/**
			 * Creation of the new basic window
			 */
			BasicWindow basicWindow = new BasicWindow();
			if(circularCache.size() > 0) {
				basicWindow.startingTimestamp = circularCache.getFirst().startingTimestamp + slide;
			}else {
				basicWindow.startingTimestamp = currentTimestamp;
			}
			basicWindow.endingTimestamp = basicWindow.startingTimestamp + windowSize;
			basicWindow.tuples = new HashMap<String, ArrayList<Values>>();
			ArrayList<Values> tupleList = new ArrayList<Values>();
			tupleList.add(tuple);
			basicWindow.tuples.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), tupleList);
			basicWindow.basicWindowStateSize = tuple.toArray().toString().length();
			basicWindow.numberOfTuples = 1;
			circularCache.addFirst(basicWindow);
			stateByteSize += tuple.toArray().toString().length();
			totalNumberOfTuples += 1;
		}
	}
	
	/**
	 * method for attempting to join a tuple of the different relation
	 * @param currentTimestamp the given timestamp of arrival for the tuple in milliseconds
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
			if(basicWindow.endingTimestamp >= currentTimestamp) {
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
				 * If we overcome the current window (go to not matching windows), the loop breaks and the result 
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
	
	public long getStateSize() {
		return stateByteSize;
	}
	
	public long getNumberOfTuples() {
		return totalNumberOfTuples;
	}
	
}
