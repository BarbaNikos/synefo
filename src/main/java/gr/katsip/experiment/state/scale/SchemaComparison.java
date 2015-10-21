package gr.katsip.experiment.state.scale;

import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.colocated.BasicCollocatedEquiWindow;
import gr.katsip.tpch.LineItem;
import gr.katsip.tpch.Order;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by katsip on 10/20/2015.
 */
public class SchemaComparison {
    public static void main(String args[]) {
        LinkedList<BasicCollocatedEquiWindow> ring = new LinkedList<>();
        BasicCollocatedEquiWindow window = new BasicCollocatedEquiWindow();
        window.byteStateSize = 12345;
        ring.addFirst(window);
        System.out.println(ring.getFirst().byteStateSize);
        ring.getFirst().byteStateSize += (234);
        System.out.println(ring.getFirst().byteStateSize);
    }
}
