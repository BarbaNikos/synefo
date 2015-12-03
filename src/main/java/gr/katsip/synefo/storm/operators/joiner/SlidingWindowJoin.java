package gr.katsip.synefo.storm.operators.joiner;

import java.io.Serializable;
import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingWindowJoin implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SlidingWindowJoin.class);

    private long windowSize;
	
	private long slide;
	
	private LinkedList<BasicEqualityWindow> ringBuffer;
	
	private int bufferSize;
	
	private Fields schema;
	
	private String joinAttribute;
	
	private long stateByteSize;
	
	private long totalNumberOfTuples;
	
	private String storedRelation;

	private String otherRelation;
	
	/**
	 * 
	 * @param windowSize size of the window in milliseconds
	 * @param slide size of slide in milliseconds
	 * @param schema the schema of the tuples that will be stored
	 * @param joinAttribute the name of the join attribute
	 */
	public SlidingWindowJoin(long windowSize, long slide, Fields schema, String joinAttribute, String storedRelation,
							 String otherRelation) {
		ringBuffer = new LinkedList<BasicEqualityWindow>();
		this.windowSize = windowSize;
		this.slide = slide;
		this.bufferSize = (int) Math.ceil(this.windowSize / this.slide);
//        logger.info("circular-cache of size (" + bufferSize + ")");
		this.schema = new Fields(schema.toList());
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
		if (ringBuffer.size() == 0) {
			/**
			 * Creation of the new basic window
			 */
//			logger.info("ring buffer is empty. Need to add the first window (start: " + currentTimestamp + ", end: " +
//				(currentTimestamp + slide) + ") and add tuple with key: " + tuple.get(schema.fieldIndex(joinAttribute)) + ".");
			BasicEqualityWindow basicWindow = new BasicEqualityWindow();
			basicWindow.start = currentTimestamp;
			basicWindow.end = basicWindow.start + slide;
			ArrayList<Values> tupleList = new ArrayList<Values>();
			tupleList.add(tuple);
			basicWindow.tuples.put((String) tuple.get(schema.fieldIndex(joinAttribute)), tupleList);
			basicWindow.basicWindowStateSize = tuple.toArray().toString().length();
			basicWindow.numberOfTuples = 1;
			ringBuffer.addFirst(basicWindow);
			stateByteSize += tuple.toArray().toString().length();
			totalNumberOfTuples = 1;
		}else if (ringBuffer.size() > 0 && ringBuffer.getFirst().start <= currentTimestamp &&
				ringBuffer.getFirst().end >= currentTimestamp) {
			/**
			 * Insert tuple in the first window, if the window is still valid
			 */
//			logger.info("first window of ringbuffer matches current-timestamp: " + currentTimestamp + "(start: " +
//					ringBuffer.getFirst().start + ", end: " + currentTimestamp + ")");
			ArrayList<Values> tupleList = null;
			int sizeBefore = 0;
			if(ringBuffer.getFirst().tuples.containsKey(tuple.get(schema.fieldIndex(joinAttribute)))) {
				tupleList = ringBuffer.getFirst().tuples.get(tuple.get(schema.fieldIndex(joinAttribute)));
				sizeBefore = ringBuffer.getFirst().tuples.get(tuple.get(schema.fieldIndex(joinAttribute))).size();
			}else {
				tupleList = new ArrayList<>();
			}
			tupleList.add(tuple);
			ringBuffer.getFirst().basicWindowStateSize += tuple.toArray().toString().length();
			ringBuffer.getFirst().tuples.put((String) tuple.get(schema.fieldIndex(joinAttribute)), tupleList);
			int sizeAfter = ringBuffer.getFirst().tuples.get(tuple.get(schema.fieldIndex(joinAttribute))).size();
			ringBuffer.getFirst().numberOfTuples += 1;
			totalNumberOfTuples += 1;
			stateByteSize += tuple.toArray().toString().length();
//			logger.info("successfully added tuple in the first window of the ring buffer (size after: " + sizeAfter + ", before: " + sizeBefore + ")");
		}else {
			/**
			 * Need to evict a basic-window (the last one), if we have used up all basic window slots 
			 * and the last basic-window has expired.
			 */
			if (ringBuffer.size() >= bufferSize) {
//                logger.info("ring buffer's size (" + ringBuffer.size() + ") exceeds upper limit (" + bufferSize
//						+ "). about to evict window with start: " + ringBuffer.getLast().start + " & end: " +
//						ringBuffer.getLast().end);
				BasicEqualityWindow basicWindow = ringBuffer.removeLast();
				stateByteSize -= basicWindow.basicWindowStateSize;
				totalNumberOfTuples -= basicWindow.numberOfTuples;
			}
			/**
			 * Creation of the new basic window
			 */
			BasicEqualityWindow basicWindow = new BasicEqualityWindow();
			basicWindow.start = ringBuffer.getFirst().end + 1;
			basicWindow.end = basicWindow.start + slide;
//			logger.info("ring buffer allocated a new window with start: " + basicWindow.start + " & end: " +
//					basicWindow.end);
			ArrayList<Values> tupleList = new ArrayList<Values>();
			tupleList.add(tuple);
			basicWindow.tuples.put((String) tuple.get(schema.fieldIndex(joinAttribute)), tupleList);
			basicWindow.basicWindowStateSize = tuple.toArray().toString().length();
			basicWindow.numberOfTuples = 1;
			ringBuffer.addFirst(basicWindow);
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
//		logger.info("received tuple with key " + tupleJoinAttribute + " will attempt to join it (ct: " + currentTimestamp + ")");
		int windowIndex = 0;
		for(int i = 0; i < ringBuffer.size(); i++) {
			BasicEqualityWindow basicWindow = ringBuffer.get(i);
			if((basicWindow.start + windowSize) > currentTimestamp) {
//				logger.info("\tfound matching window with end timestamp: " + basicWindow.end + "(" + windowIndex + ")");
				if(basicWindow.tuples.containsKey(tupleJoinAttribute)) {
//					logger.info("\t window-" + basicWindow.end + "(" + windowIndex + ") contains tuples with key: " + tupleJoinAttribute);
					ArrayList<Values> storedTuples = basicWindow.tuples.get(tupleJoinAttribute);
					for(Values t : storedTuples) {
						Values joinTuple;
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
						if(result.indexOf(joinTuple) == -1) {
							result.add(joinTuple);
//							logger.info("\t added tuple {" + joinTuple.toString() + "} to the result.");
						}
					}
				}
			}else {
//				logger.info("\twindow-" + basicWindow.end + " (" + windowIndex + ") is expired compared to the timestamp: " + currentTimestamp);
				break;
			}
			windowIndex++;
		}
		return result;
	}
	
	public LinkedList<BasicEqualityWindow> getRingBuffer() {
		return ringBuffer;
	}
	
	public long getStateSize() {
		return stateByteSize;
	}
	
	public long getNumberOfTuples() {
		return totalNumberOfTuples;
	}

	public BasicEqualityWindow getStatePart() {
        if (ringBuffer.size() > 0) {
            Random rand = new Random();
            int index = rand.nextInt(ringBuffer.size());
            BasicEqualityWindow window = ringBuffer.remove(index);
            stateByteSize -= window.basicWindowStateSize;
            totalNumberOfTuples -= window.numberOfTuples;
            return window;
        }else {
            return new BasicEqualityWindow();
        }
	}

	public void initializeState(HashMap<String, ArrayList<Values>> state) {
		ringBuffer.clear();
        stateByteSize = 0L;
        totalNumberOfTuples = 0;
        BasicEqualityWindow window = new BasicEqualityWindow();
        window.start = System.currentTimeMillis();
        window.end = window.start + slide;
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
        ringBuffer.add(window);
	}
}
