package gr.katsip.experiment.state.scale;

import backtype.storm.tuple.Fields;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;

import java.util.Arrays;

/**
 * Created by katsip on 10/20/2015.
 */
public class SchemaComparison {
    public static void main(String args[]) {
        Fields schema = new Fields(LineItem.schema);
        Fields anotherSchema = new Fields(LineItem.schema);
        Fields orderSchema = new Fields(Order.schema);
        System.out.println("schema: " + schema.toList().toString());
        System.out.println("another schema: " + anotherSchema.toString());
        System.out.println("are they equal: " + (schema.toList().toString().equals(anotherSchema.toList().toString())));
        System.out.println(Arrays.equals(schema.toList().toArray(), anotherSchema.toList().toArray()));
        System.out.println(Arrays.equals(schema.toList().toArray(), orderSchema.toList().toArray()));
    }
}
