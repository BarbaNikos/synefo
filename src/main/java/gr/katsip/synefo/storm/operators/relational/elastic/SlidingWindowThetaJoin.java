package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by katsip on 10/19/2015.
 */
public class SlidingWindowThetaJoin implements Serializable {

    Logger logger = LoggerFactory.getLogger(SlidingWindowThetaJoin.class);

    public enum ThetaCondition implements Serializable {
        LESS_THAN,
        LESS_OR_EQUAL_THAN,
        GREATER_THAN,
        GREATER_OR_EQUAL_THAN
    }

    public class BasicWindow implements Serializable {

        public Long start;

        public Long end;

        public LinkedList<Values> tuples;

        public long basicWindowStateSize;

        public long numberOfTuples;

        public BasicWindow() {
            basicWindowStateSize = 0L;
            numberOfTuples = 0L;
            tuples = new LinkedList<>();
            start = 0L;
            end = 0L;
        }
    }

    private long windowSize;

    private long slide;

    private LinkedList<BasicWindow> ringBuffer;

    private int bufferSize;

    private Fields schema;

    private String joinAttribute;

    private ThetaCondition condition;

    private long stateByteSize;

    private long totalNumberOfTuples;

    private String storedRelationName;

    private String otherRelationName;

    /**
     *
     * @param windowSize size of the window in milliseconds
     * @param slide size of slide in milliseconds
     * @param tupleSchema the schema of the tuples that will be stored
     * @param joinAttribute the name of the join attribute
     * @param condition the theta-condition
     */
    public SlidingWindowThetaJoin(long windowSize, long slide, Fields tupleSchema, String joinAttribute,
                                  String storedRelation, String otherRelation, ThetaCondition condition) {
        ringBuffer = new LinkedList<BasicWindow>();
        this.windowSize = windowSize;
        this.slide = slide;
        this.bufferSize = (int) Math.ceil(this.windowSize / this.slide);
        logger.info("circular-cache of size (" + bufferSize + ")");
        this.schema = new Fields(tupleSchema.toList());
        this.joinAttribute = joinAttribute;
        this.stateByteSize = 0L;
        this.totalNumberOfTuples = 0L;
        this.storedRelationName = storedRelation;
        this.otherRelationName = otherRelation;
        this.condition = condition;
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
            logger.info("ring buffer is empty. Need to add the first window (start: " + currentTimestamp + ", end: " +
                    (currentTimestamp + slide) + ") and add tuple with key: " + tuple.get(schema.fieldIndex(joinAttribute)) + ".");
            BasicWindow basicWindow = new BasicWindow();
            basicWindow.start = currentTimestamp;
            basicWindow.end = basicWindow.start + slide;
            basicWindow.tuples = new LinkedList<>();
            basicWindow.tuples.add(tuple);
            basicWindow.basicWindowStateSize = tuple.toArray().toString().length();
            basicWindow.numberOfTuples = 1;
            ringBuffer.addFirst(basicWindow);
            stateByteSize += tuple.toArray().toString().length();
            totalNumberOfTuples = 1;
        }else if (ringBuffer.size() > 0 && ringBuffer.getFirst().start <= currentTimestamp &&
                (ringBuffer.getFirst().end) >= currentTimestamp) {
            /**
             * Insert tuple in the first window, if the window is still valid
             */
            logger.info("first window of ringbuffer matches current-timestamp: " + currentTimestamp + "(start: " +
                    ringBuffer.getFirst().start + ", end: " + currentTimestamp + ")");
            int sizeBefore = ringBuffer.getFirst().tuples.size();
            ringBuffer.getFirst().tuples.add(tuple);
            ringBuffer.getFirst().basicWindowStateSize += tuple.toArray().toString().length();
            int sizeAfter = ringBuffer.getFirst().tuples.size();
            ringBuffer.getFirst().numberOfTuples += 1;
            totalNumberOfTuples += 1;
            stateByteSize += tuple.toArray().toString().length();
            logger.info("successfully added tuple in the first window of the ring buffer (size after: " + sizeAfter +
                    ", before: " + sizeBefore + ")");
        }else {
            /**
             * Need to evict a basic-window (the last one), if we have used up all basic window slots
             * and the last basic-window has expired.
             */
            if (ringBuffer.size() >= bufferSize) {
                logger.info("ring buffer's size (" + ringBuffer.size() + ") exceeds upper limit (" + bufferSize
                        + "). about to evict window with start: " + ringBuffer.getLast().start + " & end: " +
                        ringBuffer.getLast().end);
                BasicWindow basicWindow = ringBuffer.removeLast();
                stateByteSize -= basicWindow.basicWindowStateSize;
                totalNumberOfTuples -= basicWindow.numberOfTuples;
            }
            /**
             * Creation of the new basic window
             */
            BasicWindow basicWindow = new BasicWindow();
            if (ringBuffer.size() > 0) {
                basicWindow.start = ringBuffer.getFirst().end + 1;
            }else {
                basicWindow.start = currentTimestamp;
            }
            basicWindow.end = basicWindow.start + slide;
            logger.info("ring buffer allocated a new window with start: " + basicWindow.start + " & end: " +
                    basicWindow.end);
            basicWindow.tuples = new LinkedList<>();
            basicWindow.tuples.add(tuple);
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
        Long receivedTuple = Long.parseLong(tupleJoinAttribute);
        ArrayList<Values> result = new ArrayList<Values>();
        /**
         * Iterate over all valid windows according to the given timestamp
         */
        logger.info("received tuple with key " + tupleJoinAttribute + " will attempt to join it (ct: " + currentTimestamp + ")");
        int windowIndex = 0;
        for(int i = 0; i < ringBuffer.size(); i++) {
            BasicWindow basicWindow = ringBuffer.get(i);
            if((basicWindow.start + windowSize) > currentTimestamp) {
                logger.info("\tfound matching window with end timestamp: " + basicWindow.end + "(" + windowIndex + ")");
                for (Values t : basicWindow.tuples) {
                    Long currentValue = Long.parseLong((String) t.get(this.schema.fieldIndex(this.joinAttribute)));
                    Values joinTuple;
                    boolean condition = false;
                    switch (this.condition) {
                        case LESS_OR_EQUAL_THAN:
                            if (currentValue <= receivedTuple)
                                condition = true;
                            break;
                        case LESS_THAN:
                            if (currentValue < receivedTuple)
                                condition = true;
                            break;
                        case GREATER_OR_EQUAL_THAN:
                            if (currentValue >= receivedTuple)
                                condition = true;
                            break;
                        case GREATER_THAN:
                            if (currentValue > receivedTuple)
                                condition = true;
                            break;
                    }
                    if (condition) {
                        if (storedRelationName.compareTo(this.otherRelationName) <= 0) {
                            joinTuple = new Values(t.toArray());
                            joinTuple.addAll(tuple);
                        }else {
                            joinTuple = new Values(tuple.toArray());
                            joinTuple.addAll(t);
                        }
                        if (result.indexOf(joinTuple) < 0) {
                            result.add(joinTuple);
                            logger.info("\t added tuple {" + joinTuple.toString() + "} to the result.");
                        }
                    }
                }
            }else {
                logger.info("\twindow-" + basicWindow.end + " (" + windowIndex +
                        ") is expired compared to the timestamp: " + currentTimestamp);
                break;
            }
            windowIndex++;
        }
        return result;
    }

    public LinkedList<BasicWindow> getRingBuffer() {
        return ringBuffer;
    }

    public long getStateSize() {
        return stateByteSize;
    }

    public long getNumberOfTuples() {
        return totalNumberOfTuples;
    }

    /**
     *
     * @return
     */
    public BasicEqualityWindow getStatePart() {
        if (ringBuffer.size() > 0) {
            Random rand = new Random();
            int index = rand.nextInt(ringBuffer.size());
            BasicWindow window = ringBuffer.remove(index);
            stateByteSize -= window.basicWindowStateSize;
            totalNumberOfTuples -= window.numberOfTuples;
            /**
             * Convert it to compatible state for NewJoiner and Join bolt
             */
            HashMap<String, ArrayList<Values>> compatibleState = new HashMap<>();
            compatibleState.put("state", new ArrayList<Values>(window.tuples));
            BasicEqualityWindow compatibleWindow = new BasicEqualityWindow();
            compatibleWindow.tuples = compatibleState;
            compatibleWindow.startingTimestamp = window.start;
            compatibleWindow.endingTimestamp = window.end;
            compatibleWindow.numberOfTuples = window.numberOfTuples;
            compatibleWindow.basicWindowStateSize = window.basicWindowStateSize;
            return compatibleWindow;
        }else {
            return new BasicEqualityWindow();
        }
    }

    /**
     * @param state
     */
    public void initializeState(HashMap<String, ArrayList<Values>> state) {
        ringBuffer.clear();
        stateByteSize = 0L;
        totalNumberOfTuples = 0;
        BasicWindow window = new BasicWindow();
        window.start = System.currentTimeMillis();
        window.end = window.start + slide;
        window.tuples = new LinkedList<>(state.get("state"));
        for (Values tuple : window.tuples) {
            window.numberOfTuples++;
            window.basicWindowStateSize += tuple.toArray().toString().length();
        }
        totalNumberOfTuples = window.numberOfTuples;
        stateByteSize = window.basicWindowStateSize;
        ringBuffer.add(window);
    }

}
