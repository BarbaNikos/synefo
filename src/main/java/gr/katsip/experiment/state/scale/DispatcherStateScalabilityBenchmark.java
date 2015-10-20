package gr.katsip.experiment.state.scale;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.intellij.psi.compiled.ClassFileDecompilers;
import gr.katsip.synefo.storm.operators.relational.elastic.FullStateDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowThetaJoin;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by katsip on 10/19/2015.
 */
public class DispatcherStateScalabilityBenchmark {

    private long window;

    private long slide;

    private String orderFile;

    private String lineitemFile;

    private static final String[] schema = { "attributes", "values" };

    /**
     * constructor
     * @param window the window size in seconds
     * @param slide the slide of the window in seconds
     */
    public DispatcherStateScalabilityBenchmark(long window, long slide, String orderFile, String lineitemFile) {
        this.window = window;
        this.slide = slide;
        this.orderFile = orderFile;
        this.lineitemFile = lineitemFile;
    }

    public void benchmark() throws IOException {
        FullStateDispatcher dispatcher = new FullStateDispatcher("lineitem", new Fields(LineItem.schema),
                LineItem.schema[0], LineItem.schema[0],
                "order", new Fields(Order.schema),
                Order.schema[0], Order.schema[0], new Fields(schema));
        FullStateDispatcher orderDispatcher = new FullStateDispatcher("order", new Fields(Order.schema),
                Order.schema[0], Order.schema[0],
                "lineitem", new Fields(LineItem.schema),
                LineItem.schema[0], LineItem.schema[0], new Fields(schema));
        HashMap<String, List<Integer>> activeRelationToTaskIndex = new HashMap<>();
        List<Integer> tasks = new ArrayList<>();
        tasks.add(1);
        tasks.add(2);
        tasks.add(3);
        activeRelationToTaskIndex.put("lineitem", tasks);
        tasks = new ArrayList<>();
        tasks.add(4);
        tasks.add(5);
        tasks.add(6);
        activeRelationToTaskIndex.put("order", tasks);
        dispatcher.setTaskToRelationIndex(activeRelationToTaskIndex);
        orderDispatcher.setTaskToRelationIndex(activeRelationToTaskIndex);
        DescriptiveStatistics dispatchStatistics = new DescriptiveStatistics();
        File orders = new File(orderFile);
        BufferedReader reader = new BufferedReader(new FileReader(orders));
        String line = null;
        Fields orderSchema = new Fields(Order.schema);
        while((line = reader.readLine()) != null) {
            String[] attributes = line.split("\\|");
            Values order = new Values();
            for (String attribute : attributes) {
                order.add(attribute);
            }
            Values tuple = new Values();
            tuple.add(orderSchema);
            tuple.add(order);
            long currentTimestamp = System.currentTimeMillis();
            long dispatchStart = System.currentTimeMillis();
            orderDispatcher.execute(null, null, orderSchema, tuple);
            long dispatchEnd = System.currentTimeMillis();
            dispatchStatistics.addValue((dispatchEnd - dispatchStart));
        }
        reader.close();


        File lineitems = new File(lineitemFile);
        reader = new BufferedReader(new FileReader(lineitems));
        Fields lineitemSchema = new Fields(LineItem.schema);
//        while((line = reader.readLine()) != null) {
//            String[] attributes = line.split("\\|");
//            Values lineitem = new Values();
//            for (String attribute : attributes) {
//                lineitem.add(attribute);
//            }
//            Values tuple = new Values();
//            tuple.add(lineitemSchema);
//            tuple.add(lineitem);
//            long currentTimestamp = System.currentTimeMillis();
//            long dispatchStart = System.currentTimeMillis();
//            dispatcher.execute(null, null, lineitemSchema, tuple);
//            long dispatchEnd = System.currentTimeMillis();
//            dispatchStatistics.addValue((dispatchEnd - dispatchStart));
//        }
        reader.close();
        HashMap<Integer, Long> statistics = orderDispatcher.taskStatistics();
        Iterator<Map.Entry<Integer, Long>> iterator = statistics.entrySet().iterator();
        System.out.println("Task to key allocation: ");
        while (iterator.hasNext()) {
            Map.Entry<Integer, Long> entry = iterator.next();
            System.out.println("task-" + entry.getKey() + " number of keys: " + entry.getValue());
        }
        System.out.println("***** End of task to key allocation ****");

        System.out.println("+++ Dispatch Operation +++");
        System.out.println("State size: " + dispatcher.getStateSize());
        System.out.println("Average: " + (dispatchStatistics.getSum() / dispatchStatistics.getN()) + " msec");
        System.out.println("Mean: " + dispatchStatistics.getMean() + " msec");
        System.out.println("Min: " + dispatchStatistics.getMin() + " msec");
        System.out.println("Max: " + dispatchStatistics.getMax() + " msec");
        System.out.println("25% Percentile: " + dispatchStatistics.getPercentile(25) + " msec");
        System.out.println("50% Percentile: " + dispatchStatistics.getPercentile(50) + " msec");
        System.out.println("75% Percentile: " + dispatchStatistics.getPercentile(75) + " msec");
        System.out.println("99% Percentile: " + dispatchStatistics.getPercentile(99) + " msec");
    }
}
