package gr.katsip.synefo.storm.topology;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.cestorm.db.OperatorStatisticCollector;
import gr.katsip.synefo.storm.api.OperatorBolt;
import gr.katsip.synefo.storm.api.OperatorSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.StatCountGroupByOperator;
import gr.katsip.synefo.storm.operators.relational.StatJoinOperator;
import gr.katsip.synefo.storm.operators.relational.StatProjectOperator;
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

public class SerialOperatorTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, 
	InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		Integer zooPort = -1;
		String dbServerIp = null;
		String dbServerUser = null;
		String dbServerPass = null;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 5) {
			System.err.println("Arguments: <synefo-IP> <stream-IP> <zoo-IP> <zoo-port> <db-info-file>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
			zooPort = Integer.parseInt(args[3]);
			System.out.println("Database Configuration file provided. Parsing connection information...");
			try(BufferedReader br = new BufferedReader(new FileReader(new File(args[4])))) {
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
		OperatorStatisticCollector statCollector = new OperatorStatisticCollector(zooIP + ":" + zooPort, 
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
			ZooKeeper zk = new ZooKeeper(zooIP + ":" + zooPort, 100000, sampleWatcher);
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
		StreamgenStatTupleProducer tupleProducer = new StreamgenStatTupleProducer(streamIPs[0], 
				zooIP + ":" + zooPort, 500);
		String[] spoutSchema = { "one", "two", "three", "four", "five" };
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1", 
				new OperatorSpout("spout_1", synefoIP, synefoPort, tupleProducer), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("project_1");
		topology.put("spout_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_1", "n/a", queryId, 0, 1, "SPOUT");
		/**
		 * Stage 1: Project Operators
		 */
		StatProjectOperator projectOperator = new StatProjectOperator(new Fields(spoutSchema), 
				zooIP + ":" + zooPort, 500);
		projectOperator.setOutputSchema(new Fields(spoutSchema));
		builder.setBolt("project_1", 
				new OperatorBolt("project_1", synefoIP, synefoPort, projectOperator), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_1");
		topology.put("project_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("project_1", "n/a", queryId, 1, 2, "BOLT");
		_tmp = null;
		/**
		 * Stage 2: Join operators
		 */
		StatJoinOperator<String> joinOperator = new StatJoinOperator<String>(new StringComparator(), 100, "three", 
				new Fields(spoutSchema), new Fields(spoutSchema), zooIP + ":" + zooPort, 500);
		builder.setBolt("join_1", 
				new OperatorBolt("join_1", synefoIP, synefoPort, joinOperator), 1)
				.setNumTasks(1)
				.directGrouping("project_1");
		_tmp = new ArrayList<String>();
		_tmp.add("count_grpby");
		topology.put("join_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("join_1", "n/a", queryId, 1, 2, "BOLT");
		_tmp = null;
		/**
		 * Stage 3: Aggregate operator
		 */
		String[] groupByAttributes = new String[joinOperator.getOutputSchema().toList().size()];
		groupByAttributes = joinOperator.getOutputSchema().toList().toArray(groupByAttributes);
		StatCountGroupByOperator countGroupByAggrOperator = new StatCountGroupByOperator(100, 
				groupByAttributes, zooIP + ":" + zooPort, 500);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_grpby", 
				new OperatorBolt("count_grpby", synefoIP, synefoPort, countGroupByAggrOperator), 1)
				.setNumTasks(1)
				.directGrouping("join_1");
		topology.put("count_grpby", new ArrayList<String>());
		ceDb.insertOperator("count_grpby", "n/a", queryId, 1, 2, "BOLT");
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

		conf.setDebug(false);
		conf.setNumWorkers(4);
		StormSubmitter.submitTopology("operator-serial-top", conf, builder.createTopology());
		statCollector.init();
	}

}
