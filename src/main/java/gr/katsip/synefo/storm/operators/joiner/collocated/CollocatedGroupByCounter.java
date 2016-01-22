package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by katsip on 1/22/2016.
 */
public class CollocatedGroupByCounter {

    Logger logger = LoggerFactory.getLogger(CollocatedGroupByCounter.class);

    private String relation;

    private Fields relationSchema;

    private String groupByAttribute;

    private Fields outputSchema;

    private Fields groupByCountSchema;

    private int windowSize;

    private int slide;

    private List<String> migratedKeys;

    CollocatedWindowGroupByCount groupByCount;

    public CollocatedGroupByCounter(String relation, Fields relationSchema, String groupByAttribute, int window, int slide) {
        this.relation = relation;
        this.relationSchema = new Fields(relationSchema.toList());
        this.groupByAttribute = groupByAttribute;
        migratedKeys = new ArrayList<>();
        String[] fields = { groupByAttribute, "count" };
        this.groupByCountSchema = new Fields(fields);
        this.windowSize = window;
        this.slide = slide;
        groupByCount = new CollocatedWindowGroupByCount(windowSize, slide, relationSchema, groupByAttribute, relation);
    }

    public Pair<Integer, Integer> execute(String streamId, Tuple anchor, OutputCollector collector, List<Integer> activeTasks,
                                          Integer taskIndex, Fields fields, Values values, List<Long> times) {
        Integer numberOfTuplesProduced = 0;
        Long t1 = System.currentTimeMillis();
        groupByCount.store(t1, fields, values);
        long t2 = System.currentTimeMillis();
        List<Values> tuples = groupByCount.groupbyCount(t1, fields, values);
        long t3 = System.currentTimeMillis();
        long t4 = -1;
        if (migratedKeys.size() > 0) {
            //TODO: Fill out the mirror-groupbyCount
            t4 = System.currentTimeMillis();
        }
        if (activeTasks.size() > 0) {
            for (Values tuple : tuples) {
                Values output = new Values();
                output.add(t1.toString());
                output.add(groupByCountSchema);
                output.add(tuple);
                numberOfTuplesProduced++;
                collector.emitDirect(activeTasks.get(taskIndex), streamId, anchor, output);
                taskIndex++;
                if (taskIndex >= activeTasks.size())
                    taskIndex = 0;
            }
        }
        times.add(new Long((t2 - t1)));
        times.add(new Long((t3 - t2)));
        if (t4 > -1)
            times.add(new Long((t4 - t3)));
        return new Pair<>(taskIndex, numberOfTuplesProduced);
    }

    public void setOutputSchema(Fields output_schema) {
        outputSchema = new Fields(output_schema.toList());
    }


    public long getStateSize() {
        return groupByCount.byteStateSize;
    }

    public void initializeScaleOut(Long timestamp, List<String> migratedKeys) {
        //TODO: Feel this out
    }

    public void initializeScaleIn(Long timestamp, List<String> migratedKeys) {
        //TODO: Feel this out
    }

    public void initializeBuffer() {
        //TODO: Feel this out
    }

    public long getNumberOfTuples() {
        return groupByCount.cardinality;
    }
}
