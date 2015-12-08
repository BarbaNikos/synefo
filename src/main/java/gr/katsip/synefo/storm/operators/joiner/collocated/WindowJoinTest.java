package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.tpch.Inner;
import gr.katsip.tpch.Outer;

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
            optimizedWindowEquiJoin.store(timestamp, new Fields(Inner.schema), tuple);
        }

        if (collocatedWindowEquiJoin.getNumberOfTuples() != optimizedWindowEquiJoin.getNumberOfTuples()) {
            long j = collocatedWindowEquiJoin.getNumberOfTuples();
            System.out.println("Difference: " + (Math.abs(optimizedWindowEquiJoin.getNumberOfTuples() - j)));
        }
    }
}
