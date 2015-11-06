package gr.katsip.synefo.storm.producers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.HashMap;

/**
 * Created by katsip on 11/6/2015.
 */
public class SingleThreadControlledFileProducer implements Serializable, FileProducer {

    Logger logger = LoggerFactory.getLogger(SingleThreadControlledFileProducer.class);

    private Fields fields;

    private Fields schema;

    private Fields projectedSchema;

    private String pathToFile;

    private long startTimestamp = -1L;

    private long nextTimestamp = -1L;

    private int index;

    private int[] checkpoints;

    private double[] outputRate;

    private long delay;

    private int inputRate;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    private boolean finished;

    private BufferedReader reader;

    public SingleThreadControlledFileProducer(String pathToFile, String[] schema, String[] projectedSchema,
                                              double[] outputRate, int[] checkpoints) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        this.checkpoints = checkpoints;
        this.outputRate = outputRate;
        finished = false;
    }

    @Override
    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            try {
                reader = new BufferedReader(new FileReader(input));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }else {
            logger.error("file not found");
        }
        nextTimestamp = System.nanoTime();
        startTimestamp = nextTimestamp;
        index = -1;
        inputRate = 0;
        throughputPreviousTimestamp = System.currentTimeMillis();
        finished = false;
    }

    private void progress() {
        index++;
        startTimestamp += (checkpoints[index] * 1E+9);
        delay = (long) ( 1E+9 / outputRate[index] );
    }

    @Override
    public int nextTuple(SpoutOutputCollector spoutOutputCollector, String streamId, Integer taskIdentifier,
                         HashMap<Values, Long> tupleStatistics) {
        if (finished)
            return -1;
        while (System.nanoTime() < nextTimestamp) {
            // busy wait
        }
        Values values = new Values();
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line != null) {
            String[] attributes = line.split("\\|");
            if (attributes.length < schema.size())
                return -1;
            for (int i = 0; i < schema.size(); i++) {
                if (projectedSchema.toList().contains(schema.get(i)))
                    values.add(attributes[i]);
            }
            Values tuple = new Values();
            tuple.add("");
            tuple.add(projectedSchema);
            tuple.add(values);
            tupleStatistics.put(tuple, System.currentTimeMillis());
            spoutOutputCollector.emitDirect(taskIdentifier, streamId, tuple, tuple);
            inputRate++;
            if (startTimestamp < System.nanoTime() && index < (outputRate.length - 1))
                progress();
            throughputCurrentTimestamp = System.currentTimeMillis();
            int throughput = -2;
            if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                throughput = inputRate;
                throughputPreviousTimestamp = throughputCurrentTimestamp;
                inputRate = 0;
            }
            nextTimestamp += delay;
            return throughput;
        }else {
            finished = true;
            return -1;
        }
    }

    @Override
    public void setSchema(Fields fields) {
        this.fields = new Fields(fields.toList());
    }

    @Override
    public Fields getSchema() {
        return fields;
    }

}
