package gr.katsip.synefo.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.Topology;

public class SynEFOUserInterface implements Runnable {
	
	private boolean _exit;
	
	private Topology _topology;
	
	private Topology _runningTopology;
	
	private HashMap<String, TaskStatistics> _taskUsage;
	
	private HashMap<String, SynEFOMessage> _control_map;
	
	private HashMap<String, String> _task_ips;
	
	public SynEFOUserInterface(Topology physicalTopology, Topology runningTopology, 
			HashMap<String, TaskStatistics> taskUsage, 
			HashMap<String, SynEFOMessage> control_map, 
			HashMap<String, String> task_ips) {
		_topology = physicalTopology;
		_runningTopology = runningTopology;
		_taskUsage = taskUsage;
		_control_map = control_map;
		_task_ips = task_ips;
	}

	public void run() {
		_exit = false;
		BufferedReader _input = new BufferedReader(new InputStreamReader(System.in));
		String command = null;
		System.out.println("+EFO Server started (UI). Type help for the list of commands");
		while(_exit == false) {
			try {
				System.out.print("+EFO>");
				command = _input.readLine();
				if(command != null && command.length() > 0) {
					StringTokenizer strTok = new StringTokenizer(command, " ");
					String comm = strTok.nextToken();
					parseCommand(comm, strTok);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void parseCommand(String command, StringTokenizer strTok) {
		if(command.equals("util")) {
			HashMap<String, TaskStatistics> _taskUsageCopy = null;
			synchronized(_taskUsage) {
				_taskUsageCopy = new HashMap<String, TaskStatistics>(_taskUsage);
			}
			Iterator<Entry<String, TaskStatistics>> it = _taskUsageCopy.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, TaskStatistics> pair = (Entry<String, TaskStatistics>) it.next();
				String taskId = (String) pair.getKey();
				TaskStatistics stats = (TaskStatistics) pair.getValue();
				System.out.println("\t" + taskId + ": (" + "cpu: " + stats.get_cpu_load() + 
						", mem:" + stats.get_memory() + ", avg. latency:" + stats.get_latency() +
						", throughput:" + stats.get_throughput() + ")");
			}
		}else if(command.equals("scale-out")) {
			String action = null;
			if(strTok.hasMoreTokens()) {
				action = strTok.nextToken();
			}else {
				System.out.println("+EFO error: Need to define action {add|remove} when \"scale-out\" is issued.");
				return;
			}
			String compOne = null;
			if(strTok.hasMoreTokens()) {
				compOne = strTok.nextToken();
				if(_topology._topology.containsKey(compOne) == false) {
					System.out.println("+EFO error: Need to define an existing component-A when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+EFO error: Need to define component-A (will {remove|add} task from/to downstream) when \"scale-out\" is issued.");
				return;
			}
			String compTwo = null;
			if(strTok.hasMoreTokens()) {
				compTwo = strTok.nextToken();
				if(_topology._topology.containsKey(compTwo) == false) {
					System.out.println("+EFO error: Need to define an existing component-B when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+EFO error: Need to define component-B (will be {remove|add}-ed from/to component-A downstream) when \"scale-out\" is issued.");
				return;
			}
			/**
			 * Issue the command for scaling out
			 */
			if(action.equals("add")) {
				if(_topology._topology.get(compOne).lastIndexOf(compTwo) == -1) {
					System.out.println("+EFO error: " + compTwo + " is not an available downstream task of " + compOne + ".");
					return;
				}
				synchronized(_runningTopology) {
					_runningTopology._topology.get(compOne).add(compTwo);
					_runningTopology.notifyAll();
				}

				String ip = "";
				synchronized(_task_ips) {
					ip = _task_ips.get(compTwo);
				}
				synchronized(_control_map) {
					SynEFOMessage msg = new SynEFOMessage();
					msg._type = SynEFOMessage.Type.SCLOUT;
					msg._values.put("ACTION", "ADD");
					msg._values.put("NEW_TASK", compTwo);
					msg._values.put("NEW_TASK_IP", ip);
					_control_map.put(compOne, msg);
					_control_map.notifyAll();
				}
			}else if(action.equals("remove")) {
				synchronized(_runningTopology) {
					ArrayList<String> _top = _runningTopology._topology.get(compOne);
					if(_top.lastIndexOf(compTwo) == -1) {
						System.out.println("+EFO error: " + compTwo + " is not in the active downstream of " + compOne + ".");
						return;
					}
					for(int i = 0; i < _top.size(); i++) {
						if(_top.get(i).equals(compTwo)) {
							_top.remove(i);
							break;
						}
					}
					_runningTopology._topology.put(compOne, _top);
					_runningTopology.notifyAll();
				}

				String ip = "";
				synchronized(_task_ips) {
					ip = _task_ips.get(compTwo);
				}
				
				synchronized(_control_map) {
					SynEFOMessage msg = new SynEFOMessage();
					msg._type = SynEFOMessage.Type.SCLOUT;
					msg._values.put("ACTION", "REMOVE");
					msg._values.put("NEW_TASK", compTwo);
					msg._values.put("NEW_TASK_IP", ip);
					_control_map.put(compOne, msg);
					_control_map.notifyAll();
				}
			}
		}else if(command.equals("running-top")) {
			Iterator<Entry<String, ArrayList<String>>> itr = _runningTopology._topology.entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> entry = itr.next();
				String task = entry.getKey();
				String task_ip = _task_ips.get(entry.getKey());
				ArrayList<String> downStream = entry.getValue();
				System.out.println("\tTask: " + task + " (IP: " + task_ip + ") down stream: ");
				for(String t : downStream) {
					System.out.println("\t\t" + t);
				}
			}
		}else if(command.equals("physical-top")) {
			Iterator<Entry<String, ArrayList<String>>> itr = _topology._topology.entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> entry = itr.next();
				String task = entry.getKey();
				ArrayList<String> downStream = entry.getValue();
				System.out.println("\tTask: " + task + " down stream: ");
				for(String t : downStream) {
					System.out.println("\t\t" + t);
				}
			}
		}else if(command.equals("help")) {
			/**
			 * Print help instructions
			 */
			System.out.println("Available commands:");
			System.out.println("\t util: Show current utilization levels of each task");
			System.out.println("\t scale-out <action> <component-one> <component-two>: action:{add,remove}");
			System.out.println("\t\t component-one: task that will have downstream modified");
			System.out.println("\t\t component-two: task that will either be added or removed");
			System.out.println("\t running-top: Prints out the current working topology");
			System.out.println("\t physical-top: Prints out the physical topology");
			System.out.println("\t quit: self-explanatory");
		}else if(command.equals("quit")) {
			_exit = true;
		}else {
			System.out.println("Unrecognized command. Type help to see list of commands");
		}
	}

}
