package gr.katsip.synefo.storm.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;
import gr.katsip.synefo.utils.SynefoConstant;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Nick R. Katsipoulakis
 *
 */
public class SynefoSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7244170192535254357L;

	Logger logger = LoggerFactory.getLogger(SynefoSpout.class);

	private String taskName;

	private SpoutOutputCollector _collector;

	private int taskId;

	private String taskIP;

	private ArrayList<String> downstreamTasks = null;

	private ArrayList<Integer> intDownstreamTasks = null;

	private ArrayList<String> activeDownstreamTasks = null;

	private ArrayList<Integer> intActiveDownstreamTasks = null;

	private int idx;

	private String synefoIP;

	private Integer synefoPort;

	private TaskStatistics stats;

	private AbstractTupleProducer tupleProducer;

	transient private ZooPet pet;

	private String zooIP;

	private Integer zooPort;

	private int reportCounter;

	public SynefoSpout(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractTupleProducer tupleProducer, String zooIP, Integer zooPort) {
		taskName = task_name;
		downstreamTasks = null;
		activeDownstreamTasks = null;
		synefoIP = synEFO_ip;
		synefoPort = synEFO_port;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		reportCounter = 0;
	}

	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		Socket socket;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		logger.info("+EFO-SPOUT (" + taskName + ":" + taskId + "@" + taskIP + ") in registerToSynEFO() (timestamp: " + 
				System.currentTimeMillis() + ").");
		socket = null;
		SynefoMessage msg = new SynefoMessage();
		msg._type = Type.REG;
		msg._values.put("TASK_TYPE", "SPOUT");
		msg._values.put("TASK_NAME", taskName);
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", taskIP);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_ID", Integer.toString(taskId));
		try {
			socket = new Socket(synefoIP, synefoPort);
			output = new ObjectOutputStream(socket.getOutputStream());
			input = new ObjectInputStream(socket.getInputStream());
			output.writeObject(msg);
			output.flush();
			msg = null;
			ArrayList<String> _downstream = null;
			_downstream = (ArrayList<String>) input.readObject();
			if(_downstream != null && _downstream.size() > 0) {
				downstreamTasks = new ArrayList<String>(_downstream);
				intDownstreamTasks = new ArrayList<Integer>();
				for(String task : downstreamTasks) {
					StringTokenizer strTok = new StringTokenizer(task, ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					intDownstreamTasks.add(Integer.parseInt(strTok.nextToken()));
				}
			}else {
				downstreamTasks = new ArrayList<String>();
				intDownstreamTasks = new ArrayList<Integer>();
			}
			ArrayList<String> _active_downstream = null;
			_active_downstream = (ArrayList<String>) input.readObject();
			if(_active_downstream != null && _active_downstream.size() > 0) {
				activeDownstreamTasks = new ArrayList<String>(_active_downstream);
				intActiveDownstreamTasks = new ArrayList<Integer>();
				for(String task : activeDownstreamTasks) {
					StringTokenizer strTok = new StringTokenizer(task, ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					intActiveDownstreamTasks.add(Integer.parseInt(strTok.nextToken()));
				}
				idx = 0;
			}else {
				activeDownstreamTasks = new ArrayList<String>();
				intActiveDownstreamTasks = new ArrayList<Integer>();
				idx = 0;
			}
			/**
			 * Closing channels of communication with 
			 * SynEFO server
			 */
			output.flush();
			output.close();
			input.close();
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		/**
		 * Handshake with ZooKeeper
		 */
		pet.start();
		pet.getScaleCommand();
		System.out.println("+EFO-SPOUT (" + 
				taskName + ":" + taskId + "@" + taskIP + 
				") registered to +EFO successfully (timestamp: " + System.currentTimeMillis() + ").");
	}

	public void nextTuple() {
		/**
		 * In this execution names are sent in a round-robin 
		 * fashion to all downstream tasks. The words array is 
		 * iterated from start to beginning.
		 */
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add SYNEFO_HEADER and SYNEFO_TIMESTAMP value in the beginning
			 */
			values.add("SYNEFO_HEADER");
			values.add(new Long(System.currentTimeMillis()));
			Values returnedValues = tupleProducer.nextTuple();
			for(int i = 0; i < returnedValues.size(); i++) {
				values.add(returnedValues.get(i));
			}
			_collector.emitDirect(intActiveDownstreamTasks.get(idx), values);
			if(idx >= (intActiveDownstreamTasks.size() - 1)) {
				idx = 0;
			}else {
				idx += 1;
			}
		}
		stats.updateMemory();
		stats.updateCpuLoad();
		stats.updateThroughput(1);
		if(reportCounter >= 1000) {
			logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
					") timestamp: " + System.currentTimeMillis() + ", " + 
					"cpu: " + stats.getCpuLoad() + 
					", memory: " + stats.getMemory() +  
					", input-rate: " + stats.getThroughput());
			reportCounter = 0;
		}else {
			reportCounter += 1;
		}

		String scaleCommand = "";
		synchronized(pet) {
			if(pet.pendingCommands.isEmpty() == false) {
				scaleCommand = pet.returnScaleCommand();
			}
		}
		if(scaleCommand != null && scaleCommand.length() > 0) {
			StringTokenizer strTok = new StringTokenizer(scaleCommand, "~");
			String action = strTok.nextToken();
			String taskWithIp = strTok.nextToken();
			strTok = new StringTokenizer(taskWithIp, "@");
			String taskWithId = strTok.nextToken();
			String taskIp = strTok.nextToken();
			strTok = new StringTokenizer(taskWithId, ":");
			String task = strTok.nextToken();
			Integer task_id = Integer.parseInt(strTok.nextToken());
			StringBuilder strBuild = new StringBuilder();
			strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
			idx = 0;
			if(action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") located scale-command: " + scaleCommand + ", about to update routing tables (timestamp: " + 
						System.currentTimeMillis() + ").");
				if(action.toLowerCase().equals("activate")) {
					activeDownstreamTasks.add(taskWithIp);
					intActiveDownstreamTasks.add(task_id);
				}else if(action.toLowerCase().equals("deactivate")) {
					activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
					intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
				}
			}else {
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") located scale-command: " + scaleCommand + ", about to produce punctuation tuple (timestamp: " + 
						System.currentTimeMillis() + ").");
				if(action.toLowerCase().contains("add")) {
					activeDownstreamTasks.add(taskWithIp);
					intActiveDownstreamTasks.add(task_id);
					strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION + "/");
				}else if(action.toLowerCase().contains("remove")) {
					strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION + "/");
				}
				strBuild.append(SynefoConstant.COMP_TAG + ":" + task + ":" + task_id + "/");
				strBuild.append(SynefoConstant.COMP_NUM_TAG + ":" + intActiveDownstreamTasks.size() + "/");
				strBuild.append(SynefoConstant.COMP_IP_TAG + ":" + taskIp + "/");
				/**
				 * Populate other schema fields with null values, 
				 * after SYNEFO_HEADER
				 */
				Values punctValue = new Values();
				punctValue.add(strBuild.toString());
				/**
				 * Add typical SYNEFO_TIMESTAMP value
				 */
				punctValue.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
					punctValue.add(null);
				}
				for(Integer d_task : intActiveDownstreamTasks) {
					_collector.emitDirect(d_task, punctValue);
				}
				/**
				 * In the case of removing a downstream task 
				 * we remove it after sending the punctuation tuples, so 
				 * that the removed task is notified to share state
				 */
				if(action.toLowerCase().contains("remove") && activeDownstreamTasks.indexOf(taskWithIp) >= 0) {
					activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
					intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
				}
			}
		}
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		taskId = context.getThisTaskId();
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		pet = new ZooPet(zooIP, zooPort, taskName, taskId, taskIP);
		if(activeDownstreamTasks == null && downstreamTasks == null) {
			registerToSynEFO();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.add("SYNEFO_TIMESTAMP");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}