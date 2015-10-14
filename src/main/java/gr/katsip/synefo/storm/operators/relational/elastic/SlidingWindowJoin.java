package gr.katsip.synefo.storm.operators.relational.elastic;

import java.io.Serializable;
import java.util.*;

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

        public BasicWindow() {
            basicWindowStateSize = 0L;
            numberOfTuples = 0L;
            tuples = new HashMap<>();
            startingTimestamp = 0L;
            endingTimestamp = 0L;
        }
		
	}
	
	private long windowSize;
	
	private long slide;
	
	private LinkedList<BasicWindow> circularCache;
	
	private int circularCacheSize;
	
	private Fields tupleSchema;
	
	private String joinAttribute;
	
	private long stateByteSize;
	
	private long totalNumberOfTuples;
	
	private String storedRelation;
	
	private String otherRelation;
	
	/**
	 * 
	 * @param windowSize size of the window in milliseconds
	 * @param slide size of slide in milliseconds
	 * @param tupleSchema the schema of the tuples that will be stored
	 * @param joinAttribute the name of the join attribute
	 */
	public SlidingWindowJoin(long windowSize, long slide, Fields tupleSchema, String joinAttribute, String storedRelation,
							 String otherRelation) {
		circularCache = new LinkedList<BasicWindow>();
		this.windowSize = windowSize;
		this.slide = slide;
		this.circularCacheSize = (int) Math.ceil(this.windowSize / slide);
		this.tupleSchema = new Fields(tupleSchema.toList());
		this.joinAttribute = joinAttribute;
		this.stateByteSize = 0L;
		this.totalNumberOfTuples = 0L;
		this.storedRelation = storedRelation;
		this.otherRelation = otherRelation;
	}
	
	/**
	 * This method adds a given tuple to the store
	 * @param currentTimestamp the timestamp of the insert operation in milliseconds
	 * @param tuple the about-to-be-inserted tuple
	 */
	public void insertTuple(Long currentTimestamp, Values tuple) {
		if(circularCache.size() > 0 && circularCache.getFirst().startingTimestamp <= currentTimestamp &&
				(circularCache.getFirst().startingTimestamp + slide) > currentTimestamp &&
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
			if (circularCache.size() >= circularCacheSize) {
                if (circularCache.getLast().endingTimestamp <= currentTimestamp) {
                    BasicWindow basicWindow = circularCache.removeLast();
                    stateByteSize -= basicWindow.basicWindowStateSize;
                    totalNumberOfTuples -= basicWindow.numberOfTuples;
                }
			}
			/**
			 * Creation of the new basic window
			 */
			BasicWindow basicWindow = new BasicWindow();
			if (circularCache.size() > 0) {
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
						if(storedRelation.compareTo(this.otherRelation) <= 0) {
							joinTuple = new Values(t.toArray());
							joinTuple.addAll(tuple);
						}else {
							joinTuple = new Values(tuple.toArray());
							joinTuple.addAll(t);
						}
						/**
						 * The following is to limit the number of duplicate tuples produced
						 * (extra bandwidth)
						 */
						if(result.indexOf(joinTuple) == -1)
							result.add(joinTuple);
					}
				}
			}else {
				/**
				 * If we overcome the current window (go to not matching windows), the loop breaks and the result 
				 * can be returned
				 */
//				System.out.println("+++EXPIRED+++");
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

	public BasicWindow getStatePart() {
        if (circularCache.size() > 0) {
            Random rand = new Random();
            int index = rand.nextInt(circularCache.size());
            BasicWindow window = circularCache.remove(index);
            stateByteSize -= window.basicWindowStateSize;
            totalNumberOfTuples -= window.numberOfTuples;
            return window;
        }else {
            return new BasicWindow();
        }
	}

	public void initializeState(HashMap<String, ArrayList<Values>> state) {
		circularCache.clear();
        circularCacheSize = 0;
        stateByteSize = 0L;
        totalNumberOfTuples = 0;
        BasicWindow window = new BasicWindow();
        window.startingTimestamp = System.currentTimeMillis();
        window.endingTimestamp = window.startingTimestamp + slide;
        window.tuples = state;
        Iterator<Map.Entry<String, ArrayList<Values>>> stateIterator = window.tuples.entrySet().iterator();
        while (stateIterator.hasNext()) {
            for (Values tuple : stateIterator.next().getValue()) {
                window.numberOfTuples += 1;
                window.basicWindowStateSize += tuple.toArray().toString().length();
            }
        }
        totalNumberOfTuples = window.numberOfTuples;
        stateByteSize = window.basicWindowStateSize;
        circularCache.add(window);
	}
}
