package gr.katsip.synefo.storm.producers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by Nick R. Katsipoulakis on 11/6/2015.
 */
public class SerialControlledFileProducer implements Serializable, FileProducer {

    Logger logger = LoggerFactory.getLogger(SerialControlledFileProducer.class);

    private Fields fields;

    private Fields schema;

    private Fields projectedSchema;

    private String pathToFile;

    private long nextPeriodTimestamp = -1L;

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

    private Random random;

    public SerialControlledFileProducer(String pathToFile, String[] schema, String[] projectedSchema,
                                        double[] outputRate, int[] checkpoints) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        this.checkpoints = checkpoints;
        this.outputRate = outputRate;
        finished = false;
        random = new Random();
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
        inputRate = 0;
        throughputPreviousTimestamp = System.currentTimeMillis();
        finished = false;
        nextTimestamp = System.nanoTime();
        nextPeriodTimestamp = nextTimestamp;
        index = -1;
        progress();
        nextTimestamp += delay;
    }

    private void progress() {
        index += 1;
        delay = (long) ( 1E+9 / (outputRate[index] + random.nextInt(100)) );
        if (index <= (outputRate.length - 2))
            nextPeriodTimestamp += ((long) (checkpoints[index + 1] - checkpoints[index]) * 1E+9);
        else
            nextPeriodTimestamp = Long.MAX_VALUE;
    }

    @Override
    public int nextTuple(SpoutOutputCollector spoutOutputCollector, String streamId, Integer taskIdentifier,
                         HashMap<String, Long> tupleStatistics) {
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
            if (spoutOutputCollector != null)
                spoutOutputCollector.emitDirect(taskIdentifier, streamId, tuple, tuple);
            tupleStatistics.put(tuple.toString(), System.currentTimeMillis());
            inputRate++;
            throughputCurrentTimestamp = System.currentTimeMillis();
            int throughput = -2;
            if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                throughput = inputRate;
                throughputPreviousTimestamp = throughputCurrentTimestamp;
                inputRate = 0;
            }
            if (nextPeriodTimestamp <= System.nanoTime() && index < (outputRate.length - 1))
                progress();
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
