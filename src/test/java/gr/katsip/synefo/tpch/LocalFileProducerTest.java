package gr.katsip.synefo.tpch;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by katsip on 9/17/2015.
 */
public class LocalFileProducerTest {

    private int inputRate;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    @Test
    public void testInit() throws Exception {
        long delay = 0;
        double[] rates = { 5000, 10000, 5000 };
        double[] checkpoints = { 3, 6, 10 };

        long nextTimestamp = System.nanoTime();
        long startTimestamp = nextTimestamp + ((long) checkpoints[0] * 1000 * 1000 * 1000);
        int index = 0;
        delay = (long)((1000 * 1000 * 1000) / rates[index]);
        System.out.println("delay is " + delay + " nsec");
        nextTimestamp += delay;
        inputRate = 0;
        throughputPreviousTimestamp = System.currentTimeMillis();
        int count = 1000000;
        while (count >= 0) {
            while (System.nanoTime() <= nextTimestamp) {
                // busy wait
            }
            if (startTimestamp < System.nanoTime() && index < (rates.length - 1)) {
                index++;
                startTimestamp += (checkpoints[index] * 1000 * 1000 * 1000);
                delay = (long) ((1000 * 1000 * 1000) / rates[index]);
            }
            throughputCurrentTimestamp = System.currentTimeMillis();
            if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                int throughput = inputRate;
                throughputPreviousTimestamp = throughputCurrentTimestamp;
                inputRate = 0;
                System.out.println("throughput: " + throughput);
            }else {
                inputRate++;
            }
            nextTimestamp += delay;
            count -= 1;
        }
    }

    @Test
    public void testNextTuple() throws Exception {

    }

    @Test
    public void testSetSchema() throws Exception {

    }

    @Test
    public void testGetSchema() throws Exception {

    }
}