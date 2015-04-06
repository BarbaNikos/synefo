package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;

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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OperatorSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5401244472233520294L;

	Logger logger = LoggerFactory.getLogger(OperatorSpout.class);

	private String taskName;

	private SpoutOutputCollector _collector;

	private int taskId;

	private String taskIP;

	private ArrayList<String> downstreamTasks = null;

	private ArrayList<Integer> intDownstreamTasks = null;

	private int idx;

	private String synefoIP;

	private Integer synefoPort;

	private AbstractTupleProducer tupleProducer;

	private TaskStatistics stats;

	private int reportCounter;

	public OperatorSpout(String taskName, String synefoIP, Integer synefoPort, 
			AbstractTupleProducer tupleProducer) {
		this.taskName = taskName;
		downstreamTasks = null;
		this.synefoIP = synefoIP;
		this.synefoPort = synefoPort;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		reportCounter = 0;
	}

	@Override
	public void nextTuple() {
		if(intDownstreamTasks != null && intDownstreamTasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add OPERATOR_TIMESTAMP value in the beginning
			 */
			values.add(new Long(System.currentTimeMillis()));
			Values returnedValues = tupleProducer.nextTuple();
			for(int i = 0; i < returnedValues.size(); i++) {
				values.add(returnedValues.get(i));
			}
			_collector.emitDirect(intDownstreamTasks.get(idx), values);
			if(idx >= (intDownstreamTasks.size() - 1)) {
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
	}

	@SuppressWarnings("unchecked")
	private void retrieveDownstreamTasks() {
		ArrayList<String> activeDownstreamTasks = null;
		ArrayList<Integer> intActiveDownstreamTasks = null;
		Socket socket;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		logger.info("+OPERATOR-SPOUT (" + taskName + ":" + taskId + "@" + taskIP + 
				") in retrieveDownstreamTasks().");
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
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		taskId = context.getThisTaskId();
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		if(downstreamTasks == null || intDownstreamTasks == null) {
			retrieveDownstreamTasks();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("OPERATOR_TIMESTAMP");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}
