package gr.katsip.deprecated.crypefo;

import gr.katsip.deprecated.cestorm.db.CEStormDatabaseManager;
import gr.katsip.deprecated.cestorm.db.OperatorStatisticCollector;
import gr.katsip.deprecated.deprecated.SynefoBolt;
import gr.katsip.deprecated.deprecated.SynefoSpout;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.deprecated.crypstream.Client;
import gr.katsip.deprecated.crypstream.Select;
import gr.katsip.deprecated.crypstream.Sum;
import gr.katsip.deprecated.crypstream.ModifiedJoinOperator;
import gr.katsip.deprecated.StringComparator;

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

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DemoScenarioThree {

	public static void main(String[] args) throws UnknownHostException, IOException, 
	InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
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
				"SELECT D1.name, D1.blood_pressure FROM D1, D2 WHERE D1.location = D2.location AND D1.blood_pressure >100 AND D2.name='Captain';");
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
			if(zk.exists("/SPS", false) != null ) {
				zk.delete("/SPS", -1);
				System.out.println("znode /sps deleted");
			}
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
		ArrayList<String> preds = new ArrayList<String>();
		/**
		 * Stage 0: Data Sources Spouts
		 */
		String[] punctuationSpoutSchema = { "punct" };
		CrypefoPunctuationTupleProducer punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0], "spout_sps_1", zooIP, 500);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_sps_1", 
				new SynefoSpout("spout_sps_1", synefoIP, synefoPort, punctuationTupleProducer, zooIP), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_sps_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_sps_1", "n/a", queryId, 0, 0, "SPOUT");

		String[] dataSpoutSchema = { "tuple" };
		CrypefoDataTupleProducer dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0], 1, "spout_data_1", zooIP, 500);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_1", 
				new SynefoSpout("spout_data_1", synefoIP, synefoPort, dataTupleProducer, zooIP), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		_tmp.add("select_bolt_2");
		topology.put("spout_data_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_data_1", "n/a", queryId, 0, 1, "SPOUT");

		punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[1], "spout_sps_2", zooIP, 500);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_sps_2", 
				new SynefoSpout("spout_sps_2", synefoIP, synefoPort, punctuationTupleProducer, zooIP), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_sps_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_sps_2", "n/a", queryId, 0, 2, "SPOUT");

		dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[1], 1, "spout_data_2", zooIP, 500);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_2", 
				new SynefoSpout("spout_data_2", synefoIP, synefoPort, dataTupleProducer, zooIP), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_3");
		_tmp.add("select_bolt_4");
		topology.put("spout_data_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_data_2", "n/a", queryId, 0, 3, "SPOUT");

	
		punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[2], "spout_sps_3", zooIP, 500);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_sps_3", 
				new SynefoSpout("spout_sps_3", synefoIP, synefoPort, punctuationTupleProducer, zooIP), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt_2");
		topology.put("spout_sps_3", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_sps_3", "n/a", queryId, 0, 4, "SPOUT");

		dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[2], 2, "spout_data_3", zooIP, 500);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_3", 
				new SynefoSpout("spout_data_3", synefoIP, synefoPort, dataTupleProducer, zooIP), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("sum_bolt_1");
		_tmp.add("sum_bolt_2");
		topology.put("spout_data_3", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_data_3", "n/a", queryId, 0, 5, "SPOUT");
		
		/**
		 * Stage 1: Select operators
		 */
		String zooTokenIp = zooIP.split(":")[0];
		Integer zooTokenPort = Integer.parseInt(zooIP.split(":")[1]);
		//Stream 1
		String[] selectionOutputSchema = dataSpoutSchema;
		ArrayList<Integer> returnSet = new ArrayList<Integer>();
		returnSet.add(0);
		returnSet.add(1);
		returnSet.add(2);
		Select selectOperator = new Select(returnSet, "100", 4, 3, 0, 500, "select_bolt_1", zooTokenIp, zooTokenPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		preds.add("1," + selectOperator.getAttribute() + "," + selectOperator.getPredicate());
		builder.setBolt("select_bolt_1", 
				new SynefoBolt("select_bolt_1", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_1");
		
		selectOperator = new Select(returnSet, "100", 4, 3, 0, 500, "select_bolt_2", zooTokenIp, zooTokenPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		preds.add("1,"+selectOperator.getAttribute()+","+ selectOperator.getPredicate());
		builder.setBolt("select_bolt_2", 
				new SynefoBolt("select_bolt_2", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		topology.put("select_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_1", "n/a", queryId, 1, 0, "BOLT");
		ceDb.insertOperator("select_bolt_2", "n/a", queryId, 1, 1, "BOLT");

		//From Stream 2
		selectOperator = new Select(returnSet, "Captain", 3, 0, 0, 500, "select_bolt_3", zooTokenIp, zooTokenPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		preds.add("1," + selectOperator.getAttribute() + "," + selectOperator.getPredicate());
		builder.setBolt("select_bolt_3", 
				new SynefoBolt("select_bolt_3", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_2");
		returnSet = new ArrayList<Integer>();
		selectOperator = new Select(returnSet, "Captain", 3, 0, 0, 500, "select_bolt_4", zooTokenIp, zooTokenPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		preds.add("1," + selectOperator.getAttribute() + "," + selectOperator.getPredicate());
		builder.setBolt("select_bolt_4", 
				new SynefoBolt("select_bolt_4", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_2");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("select_bolt_3", new ArrayList<String>(_tmp));
		topology.put("select_bolt_4", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_3", "n/a", queryId, 1, 2, "BOLT");
		ceDb.insertOperator("select_bolt_4", "n/a", queryId, 1, 3, "BOLT");

		/**
		 * Q1: Aggregate
		 */
		Sum sumOperator = new Sum(1, 50, 2, "sum_bolt_1", 100, zooTokenIp, zooTokenPort);
		preds.add("2," + sumOperator.getAttribute() + ",none");
		sumOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("sum_bolt_1", 
				new SynefoBolt("sum_bolt_1", synefoIP, synefoPort, sumOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_3");
		_tmp = new ArrayList<String>();
		sumOperator = new Sum(1, 50, 2, "sum_bolt_2", 100, zooTokenIp, zooTokenPort);
		preds.add("2," + sumOperator.getAttribute() + ",none");
		sumOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("sum_bolt_2", 
				new SynefoBolt("sum_bolt_2", synefoIP, synefoPort, sumOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_3");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt_2");
		topology.put("sum_bolt_1", new ArrayList<String>(_tmp));
		topology.put("sum_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("sum_bolt_1", "n/a", queryId, 1, 4, "BOLT");
		ceDb.insertOperator("sum_bolt_2", "n/a", queryId, 1, 5, "BOLT");
		
		/**
		 * Stage 2: Join Operator
		 */
		String[] vals_left = { "id" , "name" , "stairs_climed" , "steps" , "blood_pressure" , "location" };
		String[] vals_right = { "id" , "car_ID" , "car_owner" , "location" };
		ModifiedJoinOperator<String> joinOperator = new ModifiedJoinOperator<String>("join_bolt_1",new StringComparator(), 100, "location", 
				new Fields(vals_left), new Fields(vals_right), zooTokenIp, zooTokenPort, 50);
		joinOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("join_bolt_1", 
				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, 
						joinOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("select_bolt_1")
						.directGrouping("select_bolt_2")
						.directGrouping("select_bolt_3")
						.directGrouping("select_bolt_4");
		joinOperator = new ModifiedJoinOperator<String>("join_bolt_2",new StringComparator(), 100, "location", 
				new Fields(vals_left), new Fields(vals_right), zooTokenIp, zooTokenPort, 50);
		joinOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("join_bolt_2", 
				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, 
						joinOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("select_bolt_1")
						.directGrouping("select_bolt_2")
						.directGrouping("select_bolt_3")
						.directGrouping("select_bolt_4");

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		topology.put("join_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("join_bolt_1", "n/a", queryId, 2, 0, "BOLT");
		ceDb.insertOperator("join_bolt_2", "n/a", queryId, 2, 1, "BOLT");

		/**
		 * Stage 3: Client Bolt (project operator)
		 */
		ArrayList<Integer> dataPs = new ArrayList<Integer>();
		dataPs.add(1);
		dataPs.add(2);
		String[] attributes = { "Doctor" , "fit+app" };

		//////fix below to match this scenario
		Client clientOperator = new Client("client_bolt", "Fred", attributes, dataPs, 4, zooTokenIp, zooTokenPort, preds, 50);
		String[] schema = { "tuple" , "crap" };
		clientOperator.setOutputSchema(new Fields(schema));
		clientOperator.setStateSchema(new Fields(schema));
		builder.setBolt("client_bolt", 
				new SynefoBolt("client_bolt", synefoIP, synefoPort, 
						clientOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("join_bolt_1")
						.directGrouping("join_bolt_2")
						.directGrouping("spout_sps_1")
						.directGrouping("spout_sps_2");
		topology.put("client_bolt", new ArrayList<String>());
		ceDb.insertOperator("client_bolt", "n/a", queryId, 3, 0, "BOLT");
		
		clientOperator = new Client("client_bolt_2", "Fred", attributes, dataPs, 4, zooTokenIp, zooTokenPort, preds, 50);
		clientOperator.setOutputSchema(new Fields(schema));
		clientOperator.setStateSchema(new Fields(schema));
		builder.setBolt("client_bolt_2", 
				new SynefoBolt("client_bolt_2", synefoIP, synefoPort, 
						clientOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("sum_bolt_1")
						.directGrouping("sum_bolt_2")
						.directGrouping("spout_sps_3");
		topology.put("client_bolt_2", new ArrayList<String>());
		ceDb.insertOperator("client_bolt_2", "n/a", queryId, 3, 1, "BOLT");
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
		conf.setNumWorkers(16);
		StormSubmitter.submitTopology("crypefo-demo-3", conf, builder.createTopology());
		statCollector.init();
	}


}
