package gr.katsip.synefo.storm.producers;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;

/**
 * Created by katsip on 10/14/2015.
 */
public class LocalFileProducer implements Serializable, FileProducer {

    Logger logger = LoggerFactory.getLogger(LocalFileProducer.class);

    private Fields fields;

    private Fields schema;

    private Fields projectedSchema;

    private String pathToFile;

    private BufferedReader reader;

    private int inputRate;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    private boolean finished;

    public LocalFileProducer(String pathToFile, String[] schema, String[] projectedSchema) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        finished = false;
    }

    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            logger.info("file found");
            try {
                reader = new BufferedReader(new FileReader(input));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        inputRate = 0;
        throughputPreviousTimestamp = -1;
        throughputCurrentTimestamp = -1;
        finished = false;
    }

    public int nextTuple(SpoutOutputCollector spoutOutputCollector, String streamId, Integer taskIdentifier,
                         HashMap<String, Long> tupleStatistics) {
        if (throughputPreviousTimestamp == -1)
            throughputPreviousTimestamp = System.currentTimeMillis();
        Values values = new Values();
        String line = null;
        try {
            if (!finished)
                line = reader.readLine();
            else
                return -1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line != null) {
            String[] attributes = line.split("\\|");
            if(attributes.length < schema.size())
                return -2;
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
            throughputCurrentTimestamp = System.currentTimeMillis();
            int throughput = -2;
            if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                throughput = inputRate;
                throughputPreviousTimestamp = throughputCurrentTimestamp;
                inputRate = 0;
            }
            return throughput;
        }else {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finished = true;
            return -1;
        }
    }

    public void setSchema(Fields fields) {
        this.fields = new Fields(fields.toList());
    }

    public Fields getSchema() {
        return fields;
    }
}
