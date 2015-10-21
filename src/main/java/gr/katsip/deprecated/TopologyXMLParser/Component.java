package gr.katsip.deprecated.TopologyXMLParser;

import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated
 */
public class Component {

	private String type;
	
	private String name;
	
	private int executors;
	
	private int stat_report_timestamp;
	
	private List<String> upstream_tasks;
	
	public Component() {
		upstream_tasks = new ArrayList<String>();
	}

	public Component(String type, String name) {
		super();
		this.type = type;
		this.name = name;
		upstream_tasks = new ArrayList<String>();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public List<String> getUpstreamTasks() {
		return upstream_tasks;
	}
	
	public void setUpstreamTasks(List<String> tasks) {
		upstream_tasks = new ArrayList<String>(tasks);
	}

	public int getExecutors() {
		return executors;
	}

	public void setExecutors(int executors) {
		this.executors = executors;
	}

	public int getStat_report_timestamp() {
		return stat_report_timestamp;
	}

	public void setStat_report_timestamp(int stat_report_timestamp) {
		this.stat_report_timestamp = stat_report_timestamp;
	}

	public List<String> getUpstream_tasks() {
		return upstream_tasks;
	}

	public void setUpstream_tasks(List<String> upstream_tasks) {
		this.upstream_tasks = upstream_tasks;
	}

	@Override
	public String toString() {
		return "Component [type=" + type + ", name=" + name + ", executors="
				+ executors + ", stat_report_timestamp="
				+ stat_report_timestamp + ", upstream_tasks=" + upstream_tasks
				+ "]";
	}
	
}
