package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.utils.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by katsip on 10/22/2015.
 */
public class CollocatedEquiJoiner implements Serializable {

    Logger logger = LoggerFactory.getLogger(CollocatedEquiJoiner.class);

    private String innerRelation;

    private String outerRelation;

    private Fields innerRelationSchema;

    private Fields outerRelationSchema;

    private String innerRelationKey;

    private String outerRelationKey;

    private Fields outputSchema;

    private Fields joinResultSchema;

    private CollocatedWindowEquiJoin collocatedWindowEquiJoin;

//    private OptimizedWindowEquiJoin optimizedWindowEquiJoin;

    private int windowSize;

    private int slide;

    private List<String> migratedKeys;

    public CollocatedEquiJoiner(String innerRelation, Fields innerRelationSchema, String outerRelation,
                                Fields outerRelationSchema, String innerRelationKey, String outerRelationKey,
                                int window, int slide) {
        this.innerRelation = innerRelation;
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.outerRelation = outerRelation;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.outerRelationKey = outerRelationKey;
        if (this.innerRelationSchema.fieldIndex(this.innerRelationKey) < 0) {
            throw new IllegalArgumentException("Not compatible inner-relation schema with the join-attribute");
        }
        if(this.outerRelationSchema.fieldIndex(this.outerRelationKey) < 0) {
            throw new IllegalArgumentException("Not compatible outer-relation schema with the join-attribute");
        }
        String[] innerRelationArray = new String[this.innerRelationSchema.toList().size()];
        innerRelationArray = this.innerRelationSchema.toList().toArray(innerRelationArray);
        String[] outerRelationArray = new String[this.outerRelationSchema.toList().size()];
        outerRelationArray = this.outerRelationSchema.toList().toArray(outerRelationArray);
        if (this.innerRelation.compareTo(this.outerRelation) <= 0)
            this.joinResultSchema = new Fields((String[]) ArrayUtils.addAll(innerRelationArray, outerRelationArray));
        else
            this.joinResultSchema = new Fields((String[]) ArrayUtils.addAll(outerRelationArray, innerRelationArray));
        this.windowSize = window;
        this.slide = slide;
        collocatedWindowEquiJoin = new CollocatedWindowEquiJoin(this.windowSize, this.slide, this.innerRelationSchema,
                this.outerRelationSchema, this.innerRelationKey, this.outerRelationKey, this.innerRelation, this.outerRelation);
//        optimizedWindowEquiJoin = new OptimizedWindowEquiJoin(this.windowSize, this.slide, this.innerRelationSchema,
//                this.outerRelationSchema, this.innerRelationKey, this.outerRelationKey, this.innerRelation, this.outerRelation);
        migratedKeys = new ArrayList<>();
    }

    public void setOutputSchema(Fields output_schema) {
        outputSchema = new Fields(output_schema.toList());
    }

    public Pair<Integer, Integer> execute(String streamId, Tuple anchor, OutputCollector collector, List<Integer> activeTasks,
                                          Integer taskIndex, Fields fields, Values values, List<Long> times) {
        Integer numberOfTuplesProduced = 0;
        Long t1 = System.currentTimeMillis();
        collocatedWindowEquiJoin.store(t1, fields, values);
//        optimizedWindowEquiJoin.store(t1, fields, values);
        long t2 = System.currentTimeMillis();
        List<Values> tuples = collocatedWindowEquiJoin.join(t1, fields, values);
//        List<Values> tuples = optimizedWindowEquiJoin.join(t1, fields, values);
        long t3 = System.currentTimeMillis();
        long t4 = -1;
        if (migratedKeys.size() > 0) {
            List<Values> mirrorTuples = collocatedWindowEquiJoin.mirrorJoin(t1, fields, values);
            t4 = System.currentTimeMillis();
            mirrorTuples.removeAll(tuples);
            tuples.addAll(mirrorTuples);
        }
        if (activeTasks.size() > 0) {
            for (Values tuple : tuples) {
                Values output = new Values();
                output.add(t1.toString());
                output.add(joinResultSchema);
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

    public Fields getOutputSchema() {
        return joinResultSchema;
    }

    public long getStateSize() {
        return collocatedWindowEquiJoin.getStateSize();
//        return optimizedWindowEquiJoin.getStateSize();
    }

    public void initializeScaleOut(Long timestamp, List<String> migratedKeys) {
        this.migratedKeys = migratedKeys;
        collocatedWindowEquiJoin.initializeScaleOut(migratedKeys);
//        optimizedWindowEquiJoin.initScaleBuffer(timestamp, migratedKeys);
    }

    public void initializeScaleIn(Long timestamp, List<String> migratedKeys) {
        this.migratedKeys = migratedKeys;
        collocatedWindowEquiJoin.initializeScaleIn(migratedKeys);
//        optimizedWindowEquiJoin.initScaleBuffer(timestamp, migratedKeys);
    }

    public void initializeBuffer() {
        collocatedWindowEquiJoin.initializeBuffer();
//        optimizedWindowEquiJoin.initBuffer();
    }

    public long getNumberOfTuples() {
        return collocatedWindowEquiJoin.getNumberOfTuples();
//        return optimizedWindowEquiJoin.getNumberOfTuples();
    }

}
