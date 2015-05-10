package gr.katsip.cestorm.db;

import java.io.IOException;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
import java.util.List;

//import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class OperatorStatisticCollector {

	private ZooKeeper zk;

//	private Connection connection;

	private List<String> operators;

	private Watcher dataRetrieverWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();
			/**
			 * When you retrieve the path, call the getDataAndWatch() function to 
			 * retrieve data and set watch again
			 */
			if(event.getType() == Event.EventType.NodeDataChanged) {
				//Retrieve operator
				String operator = path.substring(path.lastIndexOf("/"), path.length());
				getDataAndWatch(operator);
			}
		}
	};

	public OperatorStatisticCollector(String zookeeperAddress, 
			String dbIP, String user, String password, String dbName) {
		try {
			zk = new ZooKeeper(zookeeperAddress, 1000000, dataRetrieverWatcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
//		try {
//			connection = DriverManager.getConnection(dbIP, user, password);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}

	public void init() {
		try {
			operators = zk.getChildren("/data", false);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		for(String operator : operators) {
			getDataAndWatch(operator);
		}
		System.out.println("Operators received...");
		System.out.println("Press ENTER to stop execution...");
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//	public void getChildrenAndWatch() {
	//		zk.getChildren("/data/", dataRetrieverWatcher, 
	//				getChildrenCallback, "/data/".getBytes());
	//	}
	//	
	//	private Children2Callback getChildrenCallback = new Children2Callback() {
	//		@Override
	//		public void processResult(int rc, String path, Object ctx,
	//				List<String> children, Stat stat) {
	//			switch(Code.get(rc)) {
	//			case CONNECTIONLOSS:
	//				System.out.println("getChildrenCallback(): CONNECTIONLOSS");
	//				getChildrenAndWatch();
	//				break;
	//			case NONODE:
	//				System.out.println("getChildrenCallback(): NONODE");
	//				break;
	//			case OK:
	//				/**
	//				 * children received
	//				 */
	//				for(String child : children) {
	//					if(operators.lastIndexOf(child) < 0) {
	//						operators.add(child);
	//						
	//					}
	//				}
	//				break;
	//			default:
	//				break;
	//			}
	//		}
	//	};

	public void getDataAndWatch(String operator) {
		zk.getData("/data/" + operator, true, getDataCallback, operator);
	}

	private DataCallback getDataCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				String operator = new String(data);
				System.out.println("getDataCallback(): CONNECTIONLOSS");
				getDataAndWatch(operator);
				break;
			case NONODE:
				System.out.println("getDataCallback(): NONODE");
				break;
			case OK:
				System.out.println("getDataCallback(): Successfully retrieved stats: " + 
						new String(data));
				/**
				 * TODO: Insert them to database
				 */
				break;
			default:
				System.out.println("getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}

	};

}
