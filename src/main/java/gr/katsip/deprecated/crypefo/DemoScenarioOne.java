package gr.katsip.deprecated.crypefo;

import gr.katsip.deprecated.cestorm.db.CEStormDatabaseManager;
import gr.katsip.deprecated.cestorm.db.OperatorStatisticCollector;
import gr.katsip.deprecated.deprecated.SynefoBolt;
import gr.katsip.deprecated.deprecated.SynefoSpout;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.deprecated.crypstream.Client;
import gr.katsip.deprecated.crypstream.Select;
import gr.katsip.deprecated.crypstream.Sum;

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

public class DemoScenarioOne {

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
				"SELECT SUM(steps) FROM GPS_STREAM WHERE lon=x AND lat=y");
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
		/**
		 * Stage 0: Data Sources Spouts
		 */
		
		ArrayList<String> preds = new ArrayList<String>();
		
		String[] punctuationSpoutSchema = { "punct" };
		CrypefoPunctuationTupleProducer punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0], "spout_sps", zooIP, 1000);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_sps", 
				new SynefoSpout("spout_sps", synefoIP, synefoPort, punctuationTupleProducer, zooIP), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_sps", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_sps", "n/a", queryId, 0, 0, "SPOUT");

		String[] dataSpoutSchema = { "tuple" };
		CrypefoDataTupleProducer dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0], 1, "spout_data", zooIP, 1000);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data", 
				new SynefoSpout("spout_data", synefoIP, synefoPort, dataTupleProducer, zooIP), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		_tmp.add("select_bolt_2");
		topology.put("spout_data", new ArrayList<String>(_tmp));	
		ceDb.insertOperator("spout_data", "n/a", queryId, 0, 2, "SPOUT");

		/**
		 * Stage 1: Select operator
		 */
		String zooTokenIp = zooIP.split(":")[0];
		Integer zooTokenPort = Integer.parseInt(zooIP.split(":")[1]);
		String[] selectionOutputSchema = dataSpoutSchema;
		ArrayList<Integer> returnSet = new ArrayList<Integer>();
		returnSet.add(0);
		returnSet.add(1);
		returnSet.add(2);
		Select selectOperator = new Select(returnSet, "Captain", 1, 0, 1, 1000, "select_bolt_1", zooTokenIp, zooTokenPort);
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		preds.add("1,"+selectOperator.getAttribute()+","+ selectOperator.getPredicate());
		builder.setBolt("select_bolt_1", 
				new SynefoBolt("select_bolt_1", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data");
		_tmp = new ArrayList<String>();
		_tmp.add("sum_bolt_1");
		_tmp.add("sum_bolt_2");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_1", "n/a", queryId, 1, 1, "BOLT");

		selectOperator = new Select(returnSet, "Captain", 1, 0, 1, 1000, "select_bolt_2", zooTokenIp, zooTokenPort);
		preds.add("1,"+selectOperator.getAttribute()+","+ selectOperator.getPredicate());
		selectOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_2", 
				new SynefoBolt("select_bolt_2", synefoIP, synefoPort, selectOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data");
		_tmp = new ArrayList<String>();
		_tmp.add("sum_bolt_1");
		_tmp.add("sum_bolt_2");
		topology.put("select_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_2", "n/a", queryId, 1, 2, "BOLT");
		/**
		 * Stage 2: Aggregate Operator
		 */
		Sum sumOperator = new Sum(1, 50, 4, "sum_bolt_1", 50, zooTokenIp, zooTokenPort);
		preds.add("1,"+sumOperator.getAttribute()+",none" );
		sumOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("sum_bolt_1", 
				new SynefoBolt("sum_bolt_1", synefoIP, synefoPort, sumOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("select_bolt_1")
						.directGrouping("select_bolt_2");
		_tmp = new ArrayList<String>();
		sumOperator = new Sum(1, 50, 4, "sum_bolt_2", 50, zooTokenIp, zooTokenPort);
		preds.add("1,"+sumOperator.getAttribute()+",none" );
		sumOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("sum_bolt_2", 
				new SynefoBolt("sum_bolt_2", synefoIP, synefoPort, sumOperator, 
						zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("select_bolt_1")
						.directGrouping("select_bolt_2");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("sum_bolt_1", new ArrayList<String>(_tmp));
		topology.put("sum_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("sum_bolt_1", "n/a", queryId, 3, 1, "BOLT");
		ceDb.insertOperator("sum_bolt_2", "n/a", queryId, 3, 2, "BOLT");

		/**
		 * Stage 3: Client Bolt (project operator)
		 */
		ArrayList<Integer> dataPs = new ArrayList<Integer>();
		dataPs.add(1);
		String[] attributes = {"Doctor", "fit+app"};
		Client clientOperator = new Client("client_bolt","Fred", attributes, dataPs, 4, zooTokenIp, zooTokenPort, preds, 50);
		String[] schema = {"tuple", "crap"};
		clientOperator.setOutputSchema(new Fields(schema));
		clientOperator.setStateSchema(new Fields(schema));
		builder.setBolt("client_bolt", 
				new SynefoBolt("client_bolt", synefoIP, synefoPort, 
						clientOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("sum_bolt_1")
						.directGrouping("sum_bolt_2")
						.directGrouping("spout_sps");
		topology.put("client_bolt", new ArrayList<String>());
		ceDb.insertOperator("client_bolt", "n/a", queryId, 4, 1, "BOLT");
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
		conf.setNumWorkers(11);
		StormSubmitter.submitTopology("crypefo-demo-1", conf, builder.createTopology());
		statCollector.init();
	}


}