package gr.katsip.synefo.tpch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;

import java.io.*;

/**
 * Created by katsip on 9/15/2015.
 */
public class LocalFileProducer implements AbstractTupleProducer, Serializable {

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
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            reader = new BufferedReader(new FileReader(input));
        }else {
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
            /**
             * The above sends out only the required attributes
             */
        }
        Values tuple = new Values();
        tuple.add(projectedSchema);
        tuple.add(values);
        return tuple;
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
