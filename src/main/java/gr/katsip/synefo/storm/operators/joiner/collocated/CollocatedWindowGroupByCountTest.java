package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by nick on 1/22/16.
 */
public class CollocatedWindowGroupByCountTest {

    private static String[] schema = { "key" };

    @Test
    public void testStore() throws Exception {
        testStoreForIncrementingCounters();
    }

    public void randomStoreTest() throws Exception {
        long window = 1000L;
        long slide = 10L;
        Fields relationSchema = new Fields(schema);
        CollocatedWindowGroupByCount groupByCount = new CollocatedWindowGroupByCount(window, slide, relationSchema, schema[0], "relation");
        Random random = new Random();
        for (int i = 0; i < 100000; i++) {
            Long timestamp = System.currentTimeMillis();
            String key = Integer.toString(random.nextInt(1000));
            Values tuple = new Values();
            tuple.add(key);
            groupByCount.store(timestamp, relationSchema, tuple);
            if (i % 13 == 0)
                Thread.sleep(5);
            if (i >= 99999) {
                System.out.println(groupByCount.byteStateSize + ", " + groupByCount.cardinality);
                timestamp = System.currentTimeMillis();
            }
        }
    }

    public void testStoreForIncrementingCounters() {
        long window = 10000L;
        long slide = 1000L;
        Fields relationSchema = new Fields(schema);
        CollocatedWindowGroupByCount groupByCount = new CollocatedWindowGroupByCount(window, slide, relationSchema, schema[0], "relation");
        for (int i = 0; i < 100; i++) {
            Long timestamp = System.currentTimeMillis();
            String key = "1";
            Values tuple = new Values();
            tuple.add(key);
            groupByCount.store(timestamp, relationSchema, tuple);
        }
        System.out.println("byte-state: " + groupByCount.byteStateSize + ", cardinality: " + groupByCount.cardinality);
    }

    @Test
    public void testGroupbyCount() throws Exception {
        testCountWithIncrementalCounter();
    }

    public void testCountWithIncrementalCounter() {
        long window = 10000L;
        long slide = 1000L;
        Fields relationSchema = new Fields(schema);
        CollocatedWindowGroupByCount groupByCount = new CollocatedWindowGroupByCount(window, slide, relationSchema, schema[0], "relation");
        for (int i = 0; i < 100; i++) {
            Long timestamp = System.currentTimeMillis();
            String key = "1";
            Values tuple = new Values();
            tuple.add(key);
            groupByCount.store(timestamp, relationSchema, tuple);
            ArrayList<Values> result = groupByCount.groupbyCount(timestamp, relationSchema, tuple);
            for (Values t : result) {
                System.out.println(t.toString());
            }
        }
    }
}