package gr.katsip.synefo.storm.operators.dispatcher.collocated;

import backtype.storm.tuple.Fields;
import gr.katsip.file.filegen.BalancedFileGenerator;
import gr.katsip.synefo.storm.producers.SerialControlledFileProducer;
import gr.katsip.tpch.Inner;
import gr.katsip.tpch.Outer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Created by nick on 12/1/15.
 */
public class CollocatedWindowDispatcherTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private SerialControlledFileProducer producer;

    private File temporaryFile;

    private long interval;

    @Before
    public void setUp() throws Exception {
        BalancedFileGenerator balancedFileGenerator = new BalancedFileGenerator();
        temporaryFile = folder.newFile("tmp-test.csv");
        temporaryFile.createNewFile();
        PrintWriter writer = new PrintWriter(new FileWriter(temporaryFile));
        for (int i = 0; i < 6100; i++) {
            writer.println(i);
        }
        writer.flush();
        writer.close();
        String[] schema = { "value" };
        String[] projectedSchema = schema.clone();
        /**
         * Total tuples = 10 * 6 + 1000 * 6 + 10 * 6 = 6120 tuples sent
         */
        double[] outputRate = { 100, 200 };
        int[] checkPoints = { 0, 24 };
        producer = new SerialControlledFileProducer(temporaryFile.getAbsolutePath(), schema, projectedSchema,
                outputRate, checkPoints);
        producer.init();
    }

    @Test
    public void test() throws Exception {
        long slideInMilliSeconds = 6000L;
        float windowInMinutes = (float) 0.1;
        String[] schema = { "attributes", "values" };
        CollocatedWindowDispatcher dispatcher = new CollocatedWindowDispatcher("inner",
                new Fields(Inner.schema), Inner.schema[0], "outer", new Fields(Outer.schema), Outer.schema[0],
                new Fields(schema), (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);

    }
}