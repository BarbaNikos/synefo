package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import java.io.IOException;
import java.io.Serializable;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class dataCollector implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5782301300725371212L;

	Logger logger = LoggerFactory.getLogger(dataCollector.class);

	private int bufferSize;

	private int currentBufferSize;

	private String opId;

	private String zooIP;

	private Integer zooPort;

	private ZooKeeper zk = null;

	private StringBuilder strBuild;

	/**
	 * The main class responsible for sending data to the Zookeeper cluster
	 * @param zooIP Zookeeper IP
	 * @param zooPort Zookeeper Port
	 * @param bufferSize the maximum size of records that need to be buffered before they are sent out to the Zookeeper cluster
	 * @param ID the operators ID
	 */
	public dataCollector(String zooIP, Integer zooPort, int bufferSize, String ID) {
		this.bufferSize = bufferSize;
		this.currentBufferSize = 0;
		opId = ID;
		strBuild = new StringBuilder();
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		try {
			zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, null);
			if(zk.exists("/data/" + opId, false) != null ) {
				zk.delete("/data/" + opId, -1);
			}
			zk.create("/data/"+ opId, (new String("/data/"+ opId)).getBytes(), 
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
			strBuild.append(tuple + ";");
			currentBufferSize += 1;
		}else {
			logger.info("dataCollector.addToBuffer(): About to create new child node, buffer is full (buffer-size: " + 
					strBuild.length() + ").");
			createChildNode(strBuild.toString().getBytes());
			currentBufferSize = 0;
			strBuild = new StringBuilder();
			strBuild.append(tuple);
		}
	}



	public void createChildNode(byte[] statBuffer) {
//		String newChildPath = "/data/" + opId + "/";
		String nodePath = "/data/" + opId;
		//TODO: Do we need the data twice?? Both in the /data/opId node and in the /data/opId/n node??
//		zk.create(newChildPath, statBuffer, Ids.OPEN_ACL_UNSAFE, 
//				CreateMode.PERSISTENT_SEQUENTIAL, createChildNodeCallback, statBuffer);
		zk.setData(nodePath, statBuffer, -1, setDataCallback, statBuffer);
	}
	
	StatCallback setDataCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				createChildNode(statBuffer);
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

	private StringCallback createChildNodeCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				createChildNode(statBuffer);
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
//public void startStats(int id){
//String path = "/crypto/"+id+"/plain_percent";
//ZkConnector zkc = new ZkConnector();
//try {
//	zkc.connect("localhost");
//	ZooKeeper zk = zkc.getZooKeeper();
//	zk.create(path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
//	zk.create(path5,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
//} catch (IOException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (InterruptedException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (KeeperException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
//
//}
//
//public void sendStats(int id, HashMap<String, Integer> encryptionMap){
//String path = "/crypto/"+id+"/plain_percent";
//String path2 = "/crypto/"+id+"/RND_percent";
//String path3 = "/crypto/"+id+"/DET_percent";
//String path4 = "/crypto/"+id+"/OPE_percent";
//String path5 = "/crypto/"+id+"/HOM_percent";
//
//ZkConnector zkc = new ZkConnector();
//try {
//	zkc.connect("localhost");
//	ZooKeeper zk = zkc.getZooKeeper();
//	zk.setData(path, encryptionMap.get("PLN").toString().getBytes(), 0);
//	zk.setData(path2, encryptionMap.get("RND").toString().getBytes(), 0);
//	zk.setData(path3, encryptionMap.get("DET").toString().getBytes(), 0);
//	zk.setData(path4, encryptionMap.get("OPE").toString().getBytes(), 0);
//	zk.setData(path5, encryptionMap.get("RND").toString().getBytes(), 0);
//} catch (IOException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (InterruptedException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (KeeperException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
//}
