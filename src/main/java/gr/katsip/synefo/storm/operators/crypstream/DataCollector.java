package gr.katsip.synefo.storm.operators.crypstream;

import java.io.IOException;
import java.io.Serializable;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCollector implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5782301300725371212L;

	Logger logger = LoggerFactory.getLogger(DataCollector.class);

	private int bufferSize;

	private int currentBufferSize;

	private String operatorIdentifier;

	private String zooIP;

	private Integer zooPort;

	private ZooKeeper zk = null;

	private StringBuilder strBuild;
	
	private Watcher dataCollectorWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			
		}
	};

	/**
	 * The main class responsible for sending data to the Zookeeper cluster
	 * @param zooIP Zookeeper IP
	 * @param zooPort Zookeeper Port
	 * @param bufferSize the maximum size of records that need to be buffered before they are sent out to the Zookeeper cluster
	 * @param operatorIdentifier the operators ID
	 */
	public DataCollector(String zooIP, Integer zooPort, int bufferSize, String operatorIdentifier) {
		this.bufferSize = bufferSize;
		this.currentBufferSize = 0;
		this.operatorIdentifier = operatorIdentifier;
		strBuild = new StringBuilder();
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		try {
			zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, dataCollectorWatcher);
			if(zk.exists("/data/" + this.operatorIdentifier, false) != null ) {
				zk.delete("/data/" + this.operatorIdentifier, -1);
			}
			zk.create("/data/"+ this.operatorIdentifier, (new String("/data/"+ this.operatorIdentifier)).getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public void addToBuffer(String tuple) {
		if(currentBufferSize < bufferSize) {
			currentBufferSize += 1;
		}else {
			logger.info("dataCollector.addToBuffer(): About to create new child node, buffer is full (buffer-size: " + 
					strBuild.length() + ").");
			pushStatisticData(tuple.getBytes());
			currentBufferSize = 0;
		}
	}

	public void pushStatisticData(byte[] statBuffer) {
		String nodePath = "/data/" + this.operatorIdentifier;
		zk.setData(nodePath, statBuffer, -1, setDataCallback, statBuffer);
	}
	
	StatCallback setDataCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				pushStatisticData(statBuffer);
				logger.error("dataCollector.getDataCallback(): CONNECTIONLOSS for: " + path + ". Attempting again.");
				break;
			case NONODE:
				logger.error("dataCollector.getDataCallback(): NONODE with name: " + path);
				break;
			case OK:
				logger.info("dataCollector.getDataCallback(): OK data have been set successfully.");
				break;
			default:
				logger.error("dataCollector.getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	@SuppressWarnings("unused")
	private StringCallback createChildNodeCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				pushStatisticData(statBuffer);
				logger.error("dataCollector.createChildNodeCallback(): CONNECTIONLOSS for: " + path + ". Attempting again.");
				break;
			case NONODE:
				logger.error("dataCollector.createChildNodeCallback(): NONODE with name: " + path);
				break;
			case NODEEXISTS:
				logger.error("dataCollector.createChildNodeCallback(): NODEEXISTS with name: " + path);
				break;
			case OK:
				logger.info("dataCollector.createChildNodeCallback(): OK buffer written successfully.");
				break;
			default:
				logger.error("dataCollector.createChildNodeCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
}
