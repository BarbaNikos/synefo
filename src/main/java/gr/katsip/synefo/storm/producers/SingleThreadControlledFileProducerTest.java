package gr.katsip.synefo.storm.producers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;

/**
 * Created by nick on 11/8/15.
 */
public class SingleThreadControlledFileProducerTest {

    private SingleThreadControlledFileProducer producer;

    private File temporaryFile;

    private long interval;

    @Before
    public void setUp() throws Exception {
        temporaryFile = new File("tmp-test.csv");
        if (temporaryFile.exists())
            temporaryFile.delete();
        temporaryFile.createNewFile();
        PrintWriter writer = new PrintWriter(new FileWriter(temporaryFile));
        for (int i = 0; i < 15000; i++) {
            writer.println(i);
        }
        writer.flush();
        writer.close();
        String[] schema = { "value" };
        String[] projectedSchema = schema.clone();
        /**
         * Total tuples = 10 * 6 + 1000 * 6 + 10 * 6 = 6120 tuples sent
         */
        double[] outputRate = { 10, 1000, 10, 1000, 10 };
        int[] checkPoints = { 0, 6, 12, 18, 24 };
        producer = new SingleThreadControlledFileProducer(temporaryFile.getAbsolutePath(), schema, projectedSchema,
                outputRate, checkPoints);
        producer.init();
    }

    @After
    public void tearDown() throws Exception {
        if (temporaryFile.isFile() && temporaryFile.exists())
            temporaryFile.delete();
    }

    @Test
    public void testInit() throws Exception {

    }

    @Test
    public void testNextTuple() throws Exception {
        SpoutOutputCollector collector = null;
        HashMap<Values, Long> statistics = new HashMap<>();
        long startTimestamp = System.currentTimeMillis();
        int throughput = 0, result;
        long previous = System.currentTimeMillis();
        long current;
        do {
            result = producer.nextTuple(collector, "default", 1, statistics);
            current = System.currentTimeMillis();
            if ((current - previous) >= 1000L) {
                interval = current - startTimestamp;
                System.out.println((interval / 1000L) + "," + throughput);
                previous = current;
                throughput = 0;
            }else {
                throughput++;
            }
        } while (result >= 0);
    }

    @Test
    public void testSetSchema() throws Exception {

    }

    @Test
    public void testGetSchema() throws Exception {

    }
}