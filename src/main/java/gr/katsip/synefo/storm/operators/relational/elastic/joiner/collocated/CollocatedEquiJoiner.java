package gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.utils.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
        migratedKeys = null;
    }

    public void setOutputSchema(Fields output_schema) {
        outputSchema = new Fields(output_schema.toList());
    }

    public Pair<Integer, Integer> execute(String streamId, Tuple anchor, OutputCollector collector, List<Integer> activeTasks,
                                          Integer taskIndex, Fields fields, Values values) {
        Integer numberOfTuplesProduced = 0;
        Long timestamp = System.currentTimeMillis();
        collocatedWindowEquiJoin.store(timestamp, fields, values);
        List<Values> tuples = collocatedWindowEquiJoin.join(timestamp, fields, values);
        if (migratedKeys.size() > 0) {
            List<Values> mirrorTuples = collocatedWindowEquiJoin.mirrorJoin(timestamp, fields, values);
            mirrorTuples.removeAll(tuples);
            tuples.addAll(mirrorTuples);
        }
        for (Values tuple : tuples) {
            Values output = new Values();
            output.add(timestamp.toString());
            output.add(joinResultSchema);
            output.add(tuple);
            numberOfTuplesProduced++;
            if (activeTasks.size() > 0) {
                collector.emitDirect(activeTasks.get(taskIndex), streamId, anchor, output);
                taskIndex++;
                if (taskIndex >= activeTasks.size())
                    taskIndex = 0;
            }
        }
        return new Pair<>(taskIndex, numberOfTuplesProduced);
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public long getStateSize() {
        return collocatedWindowEquiJoin.getStateSize();
    }

    public void initializeScaleOut(List<String> migratedKeys) {
        this.migratedKeys = migratedKeys;
        collocatedWindowEquiJoin.initializeScaleOut(migratedKeys);
    }

    public void initializeScaleIn(List<String> migratedKeys) {
        this.migratedKeys = migratedKeys;
        collocatedWindowEquiJoin.initializeScaleIn(migratedKeys);
    }

    public long getNumberOfTuples() {
        return collocatedWindowEquiJoin.getNumberOfTuples();
    }

}
