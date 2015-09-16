package gr.katsip.synefo.tpch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by katsip on 9/15/2015.
 */
public class LocalFileProducer implements AbstractTupleProducer, Serializable {

    Logger logger = LoggerFactory.getLogger(LocalFileProducer.class);

    private Fields fields;

    private Fields schema;

    private Fields projectedSchema;

    private String pathToFile;

    private BufferedReader reader;

    public LocalFileProducer(String pathToFile, String[] schema, String[] projectedSchema) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        reader = null;
    }

    public void init() throws FileNotFoundException {
        logger.info("LocalFileProducer.init() initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            logger.info("LocalFileProducer.init() file found.");
            reader = new BufferedReader(new FileReader(input));
        }else {
            logger.error("LocalFileProducer.init() file not found.");
            reader = null;
        }
    }

    @Override
    public Values nextTuple() {
        if (reader == null) {
            try {
                init();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        if (reader != null) {
            Values values = new Values();
            String line = null;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] attributes = line.split("\\|");
            if(attributes.length < schema.size())
                return null;
            for(int i = 0; i < schema.size(); i++) {
                if(projectedSchema.toList().contains(schema.get(i))) {
                    values.add(attributes[i]);
                }
            }
            Values tuple = new Values();
            tuple.add(projectedSchema);
            tuple.add(values);
            return tuple;
        }else {
            logger.error("LocalFileProducer.nextTuple() input stream is not ready.");
            throw new RuntimeException("LocalFileProducer.nextTuple() input file is not available.");
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
