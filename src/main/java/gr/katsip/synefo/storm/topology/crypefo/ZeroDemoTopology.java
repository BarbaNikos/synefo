package gr.katsip.synefo.storm.topology.crypefo;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.cestorm.db.OperatorStatisticCollector;
import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.relational.StatJoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import gr.katsip.synefo.storm.producers.StreamgenStatTupleProducer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ZeroDemoTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		String dbServerIp = null;
		String dbServerUser = null;
		String dbServerPass = null;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 4) {
			System.err.println("Arguments: <synefo-IP> <stream-IP> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN> <db-info-file>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
			System.out.println("Database Configuration file provided. Parsing connection information...");
			try(BufferedReader br = new BufferedReader(new FileReader(new File(args[3])))) {
				for(String line; (line = br.readLine()) != null;) {
					String[] lineTokens = line.split(":");
					if(line.contains("db-server-ip:"))
						dbServerIp = "jdbc:mysql://" + lineTokens[1] + "/";
					else if(line.contains("db-schema-name:")) 
						dbServerIp = dbServerIp + lineTokens[1];
					else if(line.contains("db-user:"))
						dbServerUser = lineTokens[1];
					else if(line.contains("db-password:"))
						dbServerPass = lineTokens[1];
					else {
						System.err.println("Invalid db-info file provided. Please use proper formatted file. Format: ");
			    		System.err.println("db-server-ip:\"proper-ip-here\"");
			    		System.err.println("db-schema-name:\"proper-schema-name-here\"");
			    		System.err.println("db-user:\"proper-username-here\"");
			    		System.err.println("db-password:\"proper-user-password-here\"");
						System.exit(1);
					}
				}
			}
		}
		/**
		 * The following two lines need to be populated with the database information
		 */
		CEStormDatabaseManager ceDb = new CEStormDatabaseManager(dbServerIp, 
				dbServerUser, dbServerPass);
		Integer queryId = ceDb.insertQuery(1, 
				"SELECT * FROM Rstream AS R, Rstream AS S WHERE R.three = S.three");
		OperatorStatisticCollector statCollector = new OperatorStatisticCollector(zooIP, 
				dbServerIp, 
				dbServerUser, dbServerPass, queryId);
		/**
		 * Create the /data z-node once for all the bolts (also clean-up previous contents)
		 */
		Watcher sampleWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {

			}
		};
		try {
			ZooKeeper zk = new ZooKeeper(zooIP, 100000, sampleWatcher);
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
		 * Stage 0: Data Sources
		 */
		String[] spoutSchema = { "num", "one", "two", "three", "four" };
		StreamgenStatTupleProducer tupleProducer = new StreamgenStatTupleProducer(streamIPs[0], 
				zooIP, 500);
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout", 
				new SynefoSpout("spout", synefoIP, synefoPort, tupleProducer, zooIP), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("spout", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout", "n/a", queryId, 0, 1, "SPOUT");
		/**
		 * Stage 1: Join operators
		 */
		StatJoinOperator<String> joinOperator = new StatJoinOperator<String>(new StringComparator(), 50, "three", 
				new Fields(spoutSchema), new Fields(spoutSchema), zooIP, 500);
		builder.setBolt("join_bolt_1", 
				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, 
						joinOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout");
		joinOperator = new StatJoinOperator<String>(new StringComparator(), 50, "three", 
				new Fields(spoutSchema), new Fields(spoutSchema), zooIP, 500);
		builder.setBolt("join_bolt_2", 
				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, 
						joinOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout");
		_tmp = new ArrayList<String>();
		_tmp.add("drain_bolt");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("join_bolt_1", "n/a", queryId, 1, 2, "BOLT");
		topology.put("join_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("join_bolt_2", "n/a", queryId, 1, 0, "BOLT");
		/**
		 * Stage 2: Drain Operator (project operator)
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		projectOperator.setOutputSchema(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		builder.setBolt("drain_bolt", 
				new SynefoBolt("drain_bolt", synefoIP, synefoPort, 
						projectOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("join_bolt_1")
						.directGrouping("join_bolt_2");
		topology.put("drain_bolt", new ArrayList<String>());
		ceDb.insertOperator("drain_bolt", "n/a", queryId, 2, 1, "BOLT");
		ceDb.insertOperatorAdjacencyList(queryId, topology);

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
		msg._values.put("QUERY_ID", Integer.toString(queryId));
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
		ceDb.destroy();
		conf.setDebug(false);
		conf.setNumWorkers(4);
		StormSubmitter.submitTopology("zero-demo-top", conf, builder.createTopology());
		statCollector.init();
	}

}
