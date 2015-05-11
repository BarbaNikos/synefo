package gr.katsip.synefo.storm.topology.crypefo;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import gr.katsip.synefo.storm.operators.synefo_comp_ops.Client;
import gr.katsip.synefo.storm.operators.synefo_comp_ops.Select;
//import gr.katsip.synefo.storm.operators.synefo_comp_ops.modifiedJoinOperator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DemoTopologyFour {

	public static void main(String[] args) throws UnknownHostException, IOException, 
	InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 4) {
			System.err.println("Arguments: <synefo-IP> <stream-IP> <zoo-IP> <zoo-port>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
			zooPort = Integer.parseInt(args[3]);
		}
		/**
		 * Create the /data z-node once for all the bolts (also clean-up previous contents)
		 */
		try {
			ZooKeeper zk = new ZooKeeper(zooIP + ":" + zooPort, 100000, null);
			if(zk.exists("/data", false) != null) {
				System.out.println("Z-Node \"/data\" exists so we need to clean it up...");
				List<String> operators = zk.getChildren("/data", false);
				if(operators != null && operators.size() > 0) {
					System.out.println("Located (" + operators.size() + ") of Z-Node \"/data\" that need to be removed.");
					for(String operator : operators) {
						List<String> dataPoints = zk.getChildren("/data/" + operator, false);
						if(dataPoints != null && dataPoints.size() > 0) {
							System.out.println("Located " + dataPoints.size() + " data points for operator " + operator + ". removing them sequentially...");
							for(String dataPoint : dataPoints) {
								zk.delete("/data/" + operator + "/" + dataPoint, -1);
							}
							System.out.println("..." + dataPoints.size() + " data points removed.");
						}
						zk.delete("/data/" + operator, -1);
						System.out.println("... operator removed.");
					}
				}
				zk.delete("/data", -1);
				System.out.println(".. \"/data\" z-node removed.");
			}
			zk.create("/data", ("/data").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.close();
		}catch(IOException e) {
			e.printStackTrace();
		}catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
		/**
		 * Start building the topology
		 */
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * Stage 0: Data Sources Spouts
		 */
		String[] punctuationSpoutSchema = { "punct" };
		CrypefoPunctuationTupleProducer punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_punctuation_tuples_1", 
				new SynefoSpout("spout_punctuation_tuples_1", synefoIP, synefoPort, punctuationTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		String[] dataSpoutSchema = { "tuple" };
		CrypefoDataTupleProducer dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0],0);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_tuples_1", 
				new SynefoSpout("spout_data_tuples_1", synefoIP, synefoPort, dataTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt_1");
		topology.put("spout_punctuation_tuples_1", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1a");
		_tmp.add("select_bolt_1b");
		topology.put("spout_data_tuples_1", new ArrayList<String>(_tmp));

		String[] punctuationSpoutTwoSchema = { "punct" };
		punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[1]);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutTwoSchema));
		builder.setSpout("spout_punctuation_tuples_2", 
				new SynefoSpout("spout_punctuation_tuples_2", synefoIP, synefoPort, punctuationTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		String[] dataSpoutTwoSchema = { "tuple" };
		dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[1],1);
		dataTupleProducer.setSchema(new Fields(dataSpoutTwoSchema));
		builder.setSpout("spout_data_tuples_2", 
				new SynefoSpout("spout_data_tuples_2", synefoIP, synefoPort, dataTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt_1");
		topology.put("spout_punctuation_tuples_2", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_2a");
		_tmp.add("select_bolt_2b");
		topology.put("spout_data_tuples_2", new ArrayList<String>(_tmp));

		/**
		 * Stage 1: Select operator
		 */
		String[] selectionOutputSchema = dataSpoutSchema;
		ArrayList<Integer> returnSet = new ArrayList<Integer>();
		returnSet.add(0);
		returnSet.add(1);
		returnSet.add(2);
		Select selectOperator = new Select(returnSet, "50", 1, 3, 0, 1000, "select_bolt_1a", zooIP, zooPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_1a", 
				new SynefoBolt("select_bolt_1a", synefoIP, synefoPort, selectOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_1");
		selectOperator = new Select(returnSet, "50", 1, 3, 0, 1000, "select_bolt_1b", zooIP, zooPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_1b", 
				new SynefoBolt("select_bolt_1b", synefoIP, synefoPort, selectOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_1");

		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("select_bolt_1a", new ArrayList<String>(_tmp));
		topology.put("select_bolt_1b", new ArrayList<String>(_tmp));

		selectOperator = new Select(returnSet, "50", 1, 3, 1, 1000, "select_bolt_2a", zooIP, zooPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_2a", 
				new SynefoBolt("select_bolt_2a", synefoIP, synefoPort, selectOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_2");
		selectOperator = new Select(returnSet, "50", 1, 3, 1, 1000, "select_bolt_2b", zooIP, zooPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_2b", 
				new SynefoBolt("select_bolt_2b", synefoIP, synefoPort, selectOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_2");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("select_bolt_2a", new ArrayList<String>(_tmp));
		topology.put("select_bolt_2b", new ArrayList<String>(_tmp));

		/**
		 * Stage 2: Join Bolt
		 */
//		String[] vals = {"one","two","three","four"};
//		modifiedJoinOperator<String> joinOperator = new modifiedJoinOperator<String>(new StringComparator(), 100, "two", 
//				new Fields(vals), new Fields(vals));
//		builder.setBolt("join_bolt_1", 
//				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, 
//						joinOperator, zooIP, zooPort, false), 1)
//						.setNumTasks(1)
//						.directGrouping("select_bolt_1a")
//						.directGrouping("select_bolt_1b")
//						.directGrouping("select_bolt_2a")
//						.directGrouping("select_bolt_2b");
//		joinOperator = new modifiedJoinOperator<String>(new StringComparator(), 100, "two", 
//				new Fields("tuple"), new Fields("tuple"));
//		builder.setBolt("join_bolt_2", 
//				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, 
//						joinOperator, zooIP, zooPort, false), 1)
//						.setNumTasks(1)
//						.directGrouping("select_bolt_1a")
//						.directGrouping("select_bolt_1b")
//						.directGrouping("select_bolt_2a")
//						.directGrouping("select_bolt_2b");
//
//		_tmp = new ArrayList<String>();
//		_tmp.add("client_bolt_1");
//		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
//		topology.put("join_bolt_2", new ArrayList<String>(_tmp));

		/**
		 * Stage 3: Client Bolt (project operator)
		 */


		ArrayList<Integer> dataPs = new ArrayList<Integer>();
		dataPs.add(0);
		String[] attributes = {"Doctor", "fit+app"};
		Client clientOperator = new Client(0,"Fred", attributes, dataPs, 3);
		String[] schema = {"tuple", "crap"};
		clientOperator.setOutputSchema(new Fields(schema));
		clientOperator.setStateSchema(new Fields(schema));
		builder.setBolt("client_bolt_1", 
				new SynefoBolt("client_bolt_1", synefoIP, synefoPort, 
						clientOperator, zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("join_bolt_1")
						.directGrouping("join_bolt_2")
						.directGrouping("spout_punctuation_tuples_1")
						.directGrouping("spout_punctuation_tuples_2");
		topology.put("client_bolt_1", new ArrayList<String>());

		/**
		 * Notify SynEFO server about the 
		 * Topology
		 */
		System.out.println("About to connect to synefo: " + synefoIP + ":" + synefoPort);
		Socket synEFOSocket = new Socket(synefoIP, synefoPort);
		ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
		ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
		SynefoMessage msg = new SynefoMessage();
		msg._values = new HashMap<String, String>();
		msg._values.put("TASK_TYPE", "TOPOLOGY");
		_out.writeObject(msg);
		_out.flush();
		Thread.sleep(100);
		_out.writeObject(topology);
		_out.flush();
		String _ack = null;
		_ack = (String) _in.readObject();
		if(_ack.equals("+EFO_ACK") == false) {
			System.err.println("+EFO returned different message other than +EFO_ACK");
			System.exit(1);
		}
		_in.close();
		_out.close();
		synEFOSocket.close();

		conf.setDebug(false);
		conf.setNumWorkers(15);
		StormSubmitter.submitTopology("crypefo-top-4", conf, builder.createTopology());

	}
}
