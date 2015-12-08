package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.deprecated.server.Synefo;
import gr.katsip.tpch.Inner;
import gr.katsip.tpch.Outer;
import org.codehaus.plexus.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by nick on 12/7/15.
 */
public class WindowJoinTest {

    public static void main(String[] args) {
        CollocatedWindowEquiJoin collocatedWindowEquiJoin = new CollocatedWindowEquiJoin((long) (10 * (60 * 1000)), 10000L,
                new Fields(Inner.schema), new Fields(Outer.schema), Inner.schema[0], Outer.schema[0], "inner", "outer");
        OptimizedWindowEquiJoin optimizedWindowEquiJoin = new OptimizedWindowEquiJoin((long) (10 * (60 * 1000)), 10000L,
                new Fields(Inner.schema), new Fields(Outer.schema), Inner.schema[0], Outer.schema[0], "inner", "outer");

        for (int i = 0; i < 100000; i++) {
            Long timestamp = System.currentTimeMillis();
            Values tuple = new Values();
            tuple.add("1");
            tuple.add(Integer.toString(i));
            collocatedWindowEquiJoin.store(timestamp, new Fields(Inner.schema), tuple);
            collocatedWindowEquiJoin.store(timestamp, new Fields(Outer.schema), tuple);
            optimizedWindowEquiJoin.store(timestamp, new Fields(Inner.schema), tuple);
            optimizedWindowEquiJoin.store(timestamp, new Fields(Outer.schema), tuple);
        }

//        if (collocatedWindowEquiJoin.getNumberOfTuples() != optimizedWindowEquiJoin.getNumberOfTuples()) {
            long j = collocatedWindowEquiJoin.getNumberOfTuples();
            System.out.println("Collocated-window-join: " + collocatedWindowEquiJoin.getNumberOfTuples());
            System.out.println("Optimized-window-join: " + optimizedWindowEquiJoin.getNumberOfTuples());
//        }
        for (int i = 100000; i < 100000 + 100; i++) {
            Long timestamp = System.currentTimeMillis();
            Values tuple = new Values();
            tuple.add("2");
            tuple.add(Integer.toString(i));
            ArrayList<Values> result = null;
            long t1 = System.currentTimeMillis();
            result = collocatedWindowEquiJoin.join(timestamp, new Fields(Inner.schema), tuple);
            long t2 = System.currentTimeMillis();

            ArrayList<Values> optimizedResult = null;
            long t3 = System.currentTimeMillis();
            optimizedResult = optimizedWindowEquiJoin.join(timestamp, new Fields(Inner.schema), tuple);
            long t4 = System.currentTimeMillis();
            Collection difference = CollectionUtils.subtract(result, optimizedResult);
            System.out.println("collocated: " + (t2 - t1) + ", optimized: " + (t4 - t3));
            if (difference.size() > 0) {
                System.out.println("difference produced: " + difference.size());
            }
        }
    }
}
