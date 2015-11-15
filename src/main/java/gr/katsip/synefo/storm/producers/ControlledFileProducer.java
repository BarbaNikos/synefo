package gr.katsip.synefo.storm.producers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by katsip on 9/15/2015.
 */
public class ControlledFileProducer implements Serializable, FileProducer {

    Logger logger = LoggerFactory.getLogger(ControlledFileProducer.class);

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

    private static final int SIZE = 100;

    private boolean finished;

    private Random random;

    public ControlledFileProducer(String pathToFile, String[] schema, String[] projectedSchema, double[] outputRate,
                                  int[] checkpoints) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        this.checkpoints = checkpoints;
        this.outputRate = outputRate;
        buffer = new ArrayBlockingQueue<>(SIZE, true);
        finished = false;
        random = new Random();
    }

    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            fileScanner = new Thread(new SourceFileProducer(buffer, EOF, pathToFile));
            fileScanner.start();
//            /**
//             * Wait until buffer is full before starting execution
//             */
//            while (buffer.remainingCapacity() > 0) {
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
        }else {
            logger.error("file not found.");
        }
        nextTimestamp = System.nanoTime();
        startTimestamp = nextTimestamp;
        index = -1;
        progressCheckpoint();
        inputRate = 0;
        throughputPreviousTimestamp = System.currentTimeMillis();
        finished = false;
    }

    private void progressCheckpoint() {
        index++;
        startTimestamp += ( checkpoints[index] * 1E+9 );
        delay = (long) ( 1E+9 / (outputRate[index] + random.nextInt(10)) );
    }

    public int nextTuple(SpoutOutputCollector spoutOutputCollector, String streamId, Integer taskIdentifier,
                         HashMap<String, Long> tupleStatistics) {
        if (finished)
            return -1;
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
            try {
                fileScanner.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finished = true;
            return -1;
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
            tupleStatistics.put(tuple.toString(), System.currentTimeMillis());
            spoutOutputCollector.emitDirect(taskIdentifier, streamId, tuple, tuple);
            inputRate++;
        }

        if (startTimestamp < System.nanoTime() && index < (outputRate.length - 1))
            progressCheckpoint();

        throughputCurrentTimestamp = System.currentTimeMillis();
        int throughput = -2;
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
