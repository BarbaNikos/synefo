package gr.katsip.synefo.tpch;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.api.SourceFileProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by katsip on 9/15/2015.
 */
public class LocalControlledFileProducer implements Serializable, FileProducer {

    Logger logger = LoggerFactory.getLogger(LocalControlledFileProducer.class);

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

    private transient Thread fileScanner;

    private ArrayBlockingQueue<String> buffer;

    private static final String EOF = new String("end of file");

    private static final int SIZE = 10000;

    public LocalControlledFileProducer(String pathToFile, String[] schema, String[] projectedSchema, double[] outputRate,
                                       int[] checkpoints) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        this.checkpoints = checkpoints;
        this.outputRate = outputRate;
        buffer = new ArrayBlockingQueue<String>(SIZE, true);
    }

    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            logger.info("file found.");
            fileScanner = new Thread(new SourceFileProducer(buffer, EOF, pathToFile));
            fileScanner.start();
            /**
             * Wait until buffer is full before starting execution
             */
            while (buffer.remainingCapacity() > 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }else {
            logger.error("file not found.");
        }
        nextTimestamp = System.nanoTime();
        startTimestamp = nextTimestamp;
        index = -1;
        progressCheckpoint();
        inputRate = 0;
        throughputPreviousTimestamp = System.currentTimeMillis();
    }

    private void progressCheckpoint() {
        index++;
        startTimestamp += (checkpoints[index] * 1000 * 1000 * 1000);
        delay = (long)((1000 * 1000 * 1000) / outputRate[index]);
    }

    public int nextTuple(SpoutOutputCollector spoutOutputCollector, Integer taskIdentifier,
                         HashMap<Values, Long> tupleStatistics) {
        while (System.nanoTime() <= nextTimestamp) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Values values = new Values();
        String line = null;
        try {
            line = buffer.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (line == EOF) {
            /**
             * file is fully scanned
             */
            try {
                fileScanner.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return -10;
        }
        if (line != null) {
            String[] attributes = line.split("\\|");
            if(attributes.length < schema.size())
                return -1;
            for(int i = 0; i < schema.size(); i++) {
                if(projectedSchema.toList().contains(schema.get(i))) {
                    values.add(attributes[i]);
                }
            }
            Values tuple = new Values();
            tuple.add("0");
            tuple.add(projectedSchema);
            tuple.add(values);
            tupleStatistics.put(tuple, System.currentTimeMillis());
            spoutOutputCollector.emitDirect(taskIdentifier, tuple, tuple);
            inputRate++;
        }

        if (startTimestamp < System.nanoTime() && index < (outputRate.length - 1))
            progressCheckpoint();

        throughputCurrentTimestamp = System.currentTimeMillis();
        int throughput = -1;
        if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
            throughput = inputRate;
            throughputPreviousTimestamp = throughputCurrentTimestamp;
            inputRate = 0;
        }
        nextTimestamp += delay;
        return throughput;
    }

    public void setSchema(Fields fields) {
        this.fields = new Fields(fields.toList());
    }

    public Fields getSchema() {
        return fields;
    }
}
