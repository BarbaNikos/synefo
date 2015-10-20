package gr.katsip.experiment.state.scale;

import backtype.storm.tuple.Fields;
import gr.katsip.synefo.tpch.LineItem;

/**
 * Created by katsip on 10/20/2015.
 */
public class SchemaComparison {
    public static void main(String args[]) {
        Fields schema = new Fields(LineItem.schema);
        Fields anotherSchema = new Fields(LineItem.schema);
        System.out.println("schema: " + schema.toList().toString());
        System.out.println("another schema: " + anotherSchema.toString());
        System.out.println("are they equal: " + (schema.toList().toString().equals(anotherSchema.toList().toString())));

        System.out.println(schema.toList().toArray());
    }
}
