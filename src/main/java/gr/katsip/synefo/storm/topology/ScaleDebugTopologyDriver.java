package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.api.ElasticFileSpout;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.CollocatedEquiJoiner;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.CollocatedJoinBolt;
import gr.katsip.synefo.storm.producers.FileProducer;
import gr.katsip.synefo.storm.producers.LocalControlledFileProducer;
import gr.katsip.synefo.storm.producers.LocalFileProducer;
import gr.katsip.synefo.storm.producers.SingleThreadControlledFileProducer;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.tpch.Inner;
import gr.katsip.tpch.Outer;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 11/2/2015.
 */
public class ScaleDebugTopologyDriver {

    private int scale;

    private boolean AUTO_SCALE;

    private String[] inputFile;

    private double[] outputRate;

    private int[] checkpoint;

    private float windowInMinutes;

    private long slideInMilliSeconds;

    private int numberOfWorkers;

    private static final String[] schema = { "attributes", "values" };

    private static final Integer synefoPort = 5555;

    private String synefoAddress;

    private String zookeeperAddress;

    private int maxSpoutPending;

    public enum DispatcherType {
        OBLIVIOUS_DISPATCH,
        WINDOW_DISPATCH,
        HISTORY_DISPATCH,
        COLLOCATED_WINDOW_DISPATCH
    }

    public enum FileReaderType {
        DEFAULT_FILE_READER,
        CONTROLLED_FILE_READER,
        SERIAL_CONTROLLED_FILE_READER
    }

    private DispatcherType type;

    private FileReaderType readerType;

    public void configure(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
            scale = Integer.parseInt(reader.readLine().split("=")[1]);
            inputFile = reader.readLine().split("=")[1].split(",");
            String[] strOutputRate = reader.readLine().split("=")[1].split(",");
            String[] strCheckpoint = reader.readLine().split("=")[1].split(",");
            outputRate = new double[strOutputRate.length];
            checkpoint = new int[strCheckpoint.length];
            for (int i = 0; i < strOutputRate.length; i++) {
                outputRate[i] = Double.parseDouble(strOutputRate[i]);
                checkpoint[i] = Integer.parseInt(strCheckpoint[i]);
            }
            windowInMinutes = Float.parseFloat(reader.readLine().split("=")[1]);
            slideInMilliSeconds = Long.parseLong(reader.readLine().split("=")[1]);
            System.out.println("window-in-minutes: " + windowInMinutes + ", slide-in-msec: " + slideInMilliSeconds);
            numberOfWorkers = Integer.parseInt(reader.readLine().split("=")[1]);
            synefoAddress = reader.readLine().split("=")[1];
            zookeeperAddress = reader.readLine().split("=")[1];
            String strType = reader.readLine().split("=")[1].toUpperCase();
            if (DispatcherType.valueOf(strType) == DispatcherType.OBLIVIOUS_DISPATCH) {
                type = DispatcherType.OBLIVIOUS_DISPATCH;
            } else if (DispatcherType.valueOf(strType) == DispatcherType.WINDOW_DISPATCH) {
                type = DispatcherType.WINDOW_DISPATCH;
            } else if (DispatcherType.valueOf(strType) == DispatcherType.COLLOCATED_WINDOW_DISPATCH) {
                type = DispatcherType.COLLOCATED_WINDOW_DISPATCH;
            } else {
                type = DispatcherType.HISTORY_DISPATCH;
            }
            String strReaderType = reader.readLine().split("=")[1].toUpperCase();
            if (FileReaderType.valueOf(strReaderType) == FileReaderType.DEFAULT_FILE_READER) {
                readerType = FileReaderType.DEFAULT_FILE_READER;
            } else if (FileReaderType.valueOf(strReaderType) == FileReaderType.CONTROLLED_FILE_READER) {
                readerType = FileReaderType.CONTROLLED_FILE_READER;
            } else if (FileReaderType.valueOf(strReaderType) == FileReaderType.SERIAL_CONTROLLED_FILE_READER) {
                readerType = FileReaderType.SERIAL_CONTROLLED_FILE_READER;
            } else {
                readerType = FileReaderType.DEFAULT_FILE_READER;
            }
            String autoScale = reader.readLine().split("=")[1];
            if (autoScale.toLowerCase().equals("true"))
                AUTO_SCALE = true;
            else
                AUTO_SCALE = false;
            maxSpoutPending = Integer.parseInt(reader.readLine().split("=")[1]);
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void submit() {
        Integer numberOfTasks = 0;
        ArrayList<String> tasks;
        HashMap<String, ArrayList<String>> topology = new HashMap<>();
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        FileProducer inner, outer;
        switch (readerType) {
            case DEFAULT_FILE_READER:
                inner = new LocalFileProducer(inputFile[0], Inner.schema, Inner.schema);
                inner.setSchema(new Fields(schema));
                outer = new LocalFileProducer(inputFile[1], Outer.schema, Outer.schema);
                outer.setSchema(new Fields(schema));
                break;
            case SERIAL_CONTROLLED_FILE_READER:
                inner = new SingleThreadControlledFileProducer(inputFile[0], Inner.schema, Inner.schema, outputRate,
                        checkpoint);
                inner.setSchema(new Fields(schema));
                outer = new SingleThreadControlledFileProducer(inputFile[1], Outer.schema, Outer.schema, outputRate,
                        checkpoint);
                outer.setSchema(new Fields(schema));
                break;
            case CONTROLLED_FILE_READER:
                inner = new LocalControlledFileProducer(inputFile[0], Inner.schema, Inner.schema, outputRate,
                        checkpoint);
                inner.setSchema(new Fields(schema));
                outer = new LocalControlledFileProducer(inputFile[1], Outer.schema, Outer.schema, outputRate,
                        checkpoint);
                outer.setSchema(new Fields(schema));
                break;
            default:
                inner = new LocalFileProducer(inputFile[0], Inner.schema, Inner.schema);
                inner.setSchema(new Fields(schema));
                outer = new LocalFileProducer(inputFile[1], Outer.schema, Outer.schema);
                outer.setSchema(new Fields(schema));
                break;
        }
        builder.setSpout("inner", new ElasticFileSpout("inner", synefoAddress, synefoPort, inner, zookeeperAddress), 1);
        builder.setSpout("outer", new ElasticFileSpout("outer", synefoAddress, synefoPort, outer, zookeeperAddress), 1);
//        numberOfTasks += 2 * scale;
        numberOfTasks += 2;
        tasks = new ArrayList<>();
        tasks.add("dispatch");
        topology.put("inner", tasks);
        topology.put("outer", new ArrayList<>(tasks));
        CollocatedWindowDispatcher collocatedWindowDispatcher = new CollocatedWindowDispatcher("inner",
                new Fields(Inner.schema), Inner.schema[0], "outer", new Fields(Outer.schema), Outer.schema[0],
                new Fields(schema), (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("dispatch", new CollocatedDispatchBolt("dispatch", synefoAddress, synefoPort,
                collocatedWindowDispatcher, zookeeperAddress, AUTO_SCALE), 1)
                .setNumTasks(1)
                .directGrouping("inner")
                .directGrouping("outer")
                .directGrouping("joiner", "joiner-control");
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("joiner");
        topology.put("dispatch", tasks);
        CollocatedEquiJoiner joiner = new CollocatedEquiJoiner("inner", new Fields(Inner.schema), "outer",
                new Fields(Outer.schema), Inner.schema[0], Outer.schema[0], (int) (windowInMinutes * (60 * 1000)),
                (int) slideInMilliSeconds);
        joiner.setOutputSchema(new Fields(schema));
        builder.setBolt("joiner", new CollocatedJoinBolt("joiner", synefoAddress, synefoPort, joiner, zookeeperAddress),
                scale)
                .setNumTasks(scale)
                .directGrouping("dispatch", "dispatch-control")
                .directGrouping("dispatch", "dispatch-data");
        numberOfTasks += scale;
        topology.put("joiner", new ArrayList<String>());
        try {
            Socket socket = new Socket(synefoAddress, synefoPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            SynefoMessage message = new SynefoMessage();
            message._values = new HashMap<>();
            message._values.put("TASK_TYPE", "TOPOLOGY");
            message._values.put("TASK_NUM", Integer.toString(numberOfTasks));
            out.writeObject(message);
            out.flush();
            Thread.sleep(100);
            out.writeObject(topology);
            out.flush();
            String ack = (String) in.readObject();
            if (!ack.equals("+EFO_ACK")) {
                System.err.println("failed to submit topology information to load balancer");
                System.exit(1);
            }
            in.close();
            out.close();
            socket.close();
        }catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conf.setDebug(false);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, numberOfWorkers);
        conf.setNumWorkers(numberOfWorkers);
        conf.setNumAckers(numberOfWorkers);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000 -Xmx8192m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true"
        );
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        try {
            StormSubmitter.submitTopology("debug-elastic-join", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
