package gr.katsip.cestorm.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class OperatorStatisticCollector {

	private ZooKeeper zk;

	private Integer queryId;

	private CopyOnWriteArrayList<String> operators;
	
	private CEStormDatabaseManager ceDb;

	/**
	 * The default watcher for capturing events on the /data 
	 * path. The two main events captured are (a) the creation of 
	 * a child z-node under the "/data" z-node, (b) the change of 
	 * the data in a child z-node under the "/data" z-node. Every time 
	 * one the aforementioned events take place, the corresponding 
	 * action is taken (and the watch is renewed)
	 */
	private Watcher dataRetrieverWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();
			/**
			 * When you retrieve the path, call the getDataAndWatch() function to 
			 * retrieve data and set watch again
			 */
//			System.out.println("Received event type: " + event.getType());
			if(event.getType() == Event.EventType.NodeDataChanged) {
				//Retrieve operator
//				System.out.println("NodeDataChanged event: " + path);
				String operator = path.substring(path.lastIndexOf("/") + 1, path.length());
				getDataAndWatch(operator);
			}else if(event.getType() == Event.EventType.NodeChildrenChanged) {
				//Retrieve new children
				getChildrenAndWatch();
			}
		}
	};

	/**
	 * The default constructor of the OperatorStatisticCollector class
	 * @param zookeeperAddress the ZooKeeper IP followed by a colon and the Port
	 * @param dbIP the MySQL server's IP
	 * @param user the MySQL username
	 * @param password the MySQL password
	 * @param queryId the Query-ID for which data from operators are accumulated
	 * @see CEStormDatabaseManager
	 */
	public OperatorStatisticCollector(String zookeeperAddress, 
			String dbIP, String user, String password, Integer queryId) {
		operators = new CopyOnWriteArrayList<String>();
		try {
			zk = new ZooKeeper(zookeeperAddress, 1000000, dataRetrieverWatcher);
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		ceDb = new CEStormDatabaseManager(dbIP, user, password);
		this.queryId = queryId;
	}

	/**
	 * The main function of the OperatorStatisticCollector for 
	 * gathering data and renewing the watches on the appropriate z-nodes
	 */
	public void init() {
		getChildrenAndWatch();
		System.out.println("Operators received...");
		System.out.println("Press ENTER to stop execution...");
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		ceDb.destroy();
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Main function for retrieving children of the "/data" z-node
	 * and setting a watch.
	 */
	public void getChildrenAndWatch() {
		zk.getChildren("/data", 
				true, 
				getChildrenCallback, 
				"/data".getBytes());
	}

	private Children2Callback getChildrenCallback = new Children2Callback() {
		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("getChildrenCallback(): CONNECTIONLOSS");
				getChildrenAndWatch();
				break;
			case NONODE:
				System.out.println("getChildrenCallback(): NONODE");
				break;
			case OK:
				/**
				 * children received
				 */
				System.out.println("getChildrenCallback: " + Arrays.toString(children.toArray()));
				
				List<String> childrenDifference = new ArrayList<String>(children);
				childrenDifference.removeAll(operators);
				operators.addAllAbsent(children);
				for(String child : childrenDifference)
					getDataAndWatch(child);
				break;
			default:
				System.out.println("getChildrenCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}
	};

	public void getDataAndWatch(String operator) {
		zk.getData("/data/" + operator, 
				true, 
				getDataCallback, 
				operator);
	}

	private DataCallback getDataCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				String operator = new String((String) ctx);
				System.out.println("getDataCallback(): CONNECTIONLOSS");
				getDataAndWatch(operator);
				break;
			case NONODE:
				System.out.println("getDataCallback(): NONODE");
				break;
			case OK:
//				System.out.println("getDataCallback(): Successfully retrieved stats { " + 
//						new String(data) + " } for operator: " + (String) ctx);
				String readableData = new String(data);
				String operatorIdentifier = (String) ctx;
				if(readableData.contains("/data") == true)
					return;
				commitToDatabase(queryId, operatorIdentifier, readableData);
				break;
			default:
				System.out.println("getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}

	};
	
	private void commitToDatabase(Integer queryId, String operator, String data) {
		String[] stats = data.split(",");
		float cpu = Float.parseFloat(stats[0]);
		float memory = Float.parseFloat(stats[1]);
		int latency = Integer.parseInt(stats[2]);
		int throughput = Integer.parseInt(stats[3]);
		float selectivity = Float.parseFloat(stats[4]);
		int plain = Integer.parseInt(stats[5]);
		int det = Integer.parseInt(stats[6]);
		int rnd = Integer.parseInt(stats[7]);
		int ope = Integer.parseInt(stats[8]);
		int hom = Integer.parseInt(stats[9]);
		ceDb.insertStatistics(queryId, operator, cpu, memory, 
				latency, throughput, selectivity, 
				plain, det, rnd, ope, hom);
	}

}
