package gr.katsip.synefo.storm.topology;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.cestorm.db.OperatorStatisticCollector;
import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
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

/**
 * The following topology simulates a SQL query as the following
 * SELECT A.three, B.three, COUNT(*) 
 * FROM STREAM A, STREAM B
 * WHERE A.three = B.three
 * GROUP BY A.three, B.three
 * 
 * @author Nick R. Katsipoulakis
 *
 */
public class StatTopKTopology {
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
		ArrayList<String> taskList;
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
				"SELECT A.three, B.three, COUNT(*) FROM STREAM A, STREAM B WHERE A.three = B.three GROUP BY A.three, B.three");
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
		
		/**
		 * Stage 0: Two input streams (spout_1, spout_2)
		 */
		StreamgenStatTupleProducer tupleProducer = new StreamgenStatTupleProducer(streamIPs[0], 
				zooIP + ":" + zooPort, 500);
		String[] spoutSchemaOne = { "one", "two", "three", "four", "five" };
		tupleProducer.setSchema(new Fields(spoutSchemaOne));
		builder.setSpout("spout_1", 
				new SynefoSpout("spout_1", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskList = new ArrayList<String>();
		taskList.add("project_1");
		taskList.add("project_2");
		taskList.add("project_3");
		topology.put("spout_1", new ArrayList<String>(taskList));
		ceDb.insertOperator("spout_1", "n/a", queryId, 0, 4, "SPOUT");
		
		tupleProducer = new StreamgenStatTupleProducer(streamIPs[1], 
				zooIP + ":" + zooPort, 500);
		String[] spoutSchemaTwo = { "1", "2", "three", "4", "5" };
		tupleProducer.setSchema(new Fields(spoutSchemaTwo));
		builder.setSpout("spout_2", 
				new SynefoSpout("spout_2", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskList = new ArrayList<String>();
		taskList.add("join_1");
		taskList.add("join_2");
		taskList.add("join_3");
		topology.put("spout_2", new ArrayList<String>(taskList));
		ceDb.insertOperator("spout_2", "n/a", queryId, 0, 0, "SPOUT");
		
		/**
		 * Stage 1: Project operators spout_1 => {project_1, 2, 3} / spout_3 => {project_4, 5, 6}
		 */
		StatProjectOperator projectOperator = new StatProjectOperator(new Fields(spoutSchemaOne), zooIP + ":" + zooPort, 500);
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_1", 
				new SynefoBolt("project_1", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new StatProjectOperator(new Fields(spoutSchemaOne), zooIP + ":" + zooPort, 500);
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_2", 
				new SynefoBolt("project_2", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new StatProjectOperator(new Fields(spoutSchemaOne), zooIP + ":" + zooPort, 500);
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_3", 
				new SynefoBolt("project_3", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		taskList = new ArrayList<String>();
		taskList.add("join_1");
		taskList.add("join_2");
		taskList.add("join_3");
		topology.put("project_1", new ArrayList<String>(taskList));
		ceDb.insertOperator("project_1", "n/a", queryId, 1, 5, "BOLT");
		topology.put("project_2", new ArrayList<String>(taskList));
		ceDb.insertOperator("project_2", "n/a", queryId, 1, 4, "BOLT");
		topology.put("project_3", new ArrayList<String>(taskList));
		ceDb.insertOperator("project_3", "n/a", queryId, 1, 3, "BOLT");
		taskList = null;
		
		/**
		 * Stage 2: Join operators between project_bolt_1:3 (spout_1) and spout_2
		 */
		StatJoinOperator<String> joinOperator = new StatJoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo), zooIP + ":" + zooPort, 500);
		builder.setBolt("join_1", 
				new SynefoBolt("join_1", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_1")
				.directGrouping("project_2")
				.directGrouping("project_3")
				.directGrouping("spout_2");
		joinOperator = new StatJoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo), zooIP + ":" + zooPort, 500);
		builder.setBolt("join_2", 
				new SynefoBolt("join_2", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_1")
				.directGrouping("project_2")
				.directGrouping("project_3")
				.directGrouping("spout_2");
		joinOperator = new StatJoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo), zooIP + ":" + zooPort, 500);
		builder.setBolt("join_3", 
				new SynefoBolt("join_3", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_1")
				.directGrouping("project_2")
				.directGrouping("project_3")
				.directGrouping("spout_2");
		taskList = new ArrayList<String>();
		taskList.add("count_grpby_1");
		taskList.add("count_grpby_2");
		taskList.add("count_grpby_3");
		topology.put("join_1", new ArrayList<String>(taskList));
		ceDb.insertOperator("join_1", "n/a", queryId, 2, 2, "BOLT");
		topology.put("join_2", new ArrayList<String>(taskList));
		ceDb.insertOperator("join_2", "n/a", queryId, 2, 1, "BOLT");
		topology.put("join_3", new ArrayList<String>(taskList));
		ceDb.insertOperator("join_3", "n/a", queryId, 2, 0, "BOLT");
		taskList = null;
		
		/**
		 * Stage 3: Count-Group-By operators between join_bolt_1:3
		 */
		String[] groupByAttributes = new String[joinOperator.getOutputSchema().toList().size()];
		groupByAttributes = joinOperator.getOutputSchema().toList().toArray(groupByAttributes);
		
		StatCountGroupByOperator countGroupByAggrOperator = new StatCountGroupByOperator(500, groupByAttributes, 
				zooIP + ":" + zooPort, 500);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_grpby_1", 
				new SynefoBolt("count_grpby_1", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_1")
				.directGrouping("join_2")
				.directGrouping("join_3");
		countGroupByAggrOperator = new StatCountGroupByOperator(500, groupByAttributes, zooIP + ":" + zooPort, 500);
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_grpby_2", 
				new SynefoBolt("count_grpby_2", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_1")
				.directGrouping("join_2")
				.directGrouping("join_3");
		countGroupByAggrOperator = new StatCountGroupByOperator(500, groupByAttributes, zooIP + ":" + zooPort, 500);
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_grpby_3", 
				new SynefoBolt("count_grpby_3", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_1")
				.directGrouping("join_2")
				.directGrouping("join_3");
		taskList = new ArrayList<String>();
		taskList.add("drain_bolt");
		topology.put("count_grpby_1", taskList);
		ceDb.insertOperator("count_grpby_1", "n/a", queryId, 3, 2, "BOLT");
		topology.put("count_grpby_2", taskList);
		ceDb.insertOperator("count_grpby_2", "n/a", queryId, 3, 1, "BOLT");
		topology.put("count_grpby_3", taskList);
		ceDb.insertOperator("count_grpby_3", "n/a", queryId, 3, 0, "BOLT");
		
		/**
		 * Stage 4: Drain Operator (project operator)
		 */
		projectOperator = new StatProjectOperator(new Fields(countGroupBySchema), zooIP + ":" + zooPort, 500);
		projectOperator.setOutputSchema(new Fields(countGroupBySchema));
		builder.setBolt("drain_bolt", 
				new SynefoBolt("drain_bolt", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("count_grpby_1")
				.directGrouping("count_grpby_2")
				.directGrouping("count_grpby_3");
		topology.put("drain_bolt", new ArrayList<String>());
		ceDb.insertOperator("drain_bolt", "n/a", queryId, 4, 1, "BOLT");
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
		conf.setNumWorkers(12);
		StormSubmitter.submitTopology("stat-top-k", conf, builder.createTopology());
		statCollector.init();
	}
}
