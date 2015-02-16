package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.utils.StormTopologyConf;
import gr.katsip.synefo.utils.SynefoConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class StandAloneExperiment {

	public static void main(String[] args) {
		String remoteStreamSourceIP = "";
		Integer remoteStreamSourcePort = -1;
		//TODO: Parse it from arguments
		String dataSourceFileName = "";
		String[] remoteSourceCommand = {
			RemoteStreamSourceConf.jvm, 
			RemoteStreamSourceConf.remoteSourceJar,
			RemoteStreamSourceConf.executionClass, 
			dataSourceFileName
		};
		BufferedReader remoteSourceOutput = null;
		Process remoteStreamSource = null;
		ProcessBuilder processBuilder = new ProcessBuilder(remoteSourceCommand);
		processBuilder.redirectErrorStream(true);
		try {
			remoteStreamSource = processBuilder.start();
			remoteSourceOutput = new BufferedReader(
					new InputStreamReader(remoteStreamSource.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		String line = null;
		try {
			while((line = remoteSourceOutput.readLine()) != null) {
				if(line.contains("INFO-0000#")) {
					StringTokenizer strTok = new StringTokenizer(line, "#");
					String url = strTok.nextToken();
					strTok = new StringTokenizer(url, ":");
					remoteStreamSourceIP = strTok.nextToken();
					remoteStreamSourcePort = Integer.parseInt(strTok.nextToken());
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("INFO: RemoteStreamSource started and listens at " + remoteStreamSourceIP + 
				":" + remoteStreamSourcePort + ".");
		
		//TODO: parse the following three from arguments
		String resourceConfFile = "";
		String zooIP = "";
		Integer zooPort = -1;
		String synefoIP = "";
		Integer synefoPort = -1;
		String[] synefoCommand = {
				SynefoConf.jvm, 
				SynefoConf.remoteSourceJar,
				SynefoConf.executionClass, 
				resourceConfFile, 
				zooIP,
				zooPort.toString()
			};
		
		BufferedReader synefoOutput = null;
		Process synefo = null;
		ProcessBuilder synefoProcessBuilder = new ProcessBuilder(synefoCommand);
		synefoProcessBuilder.redirectErrorStream(true);
		try {
			synefo = synefoProcessBuilder.start();
			synefoOutput = new BufferedReader(
					new InputStreamReader(synefo.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			while((line = synefoOutput.readLine()) != null) {
				if(line.contains("+efo-INFO#")) {
					StringTokenizer strTok = new StringTokenizer(line, "#");
					String url = strTok.nextToken();
					strTok = new StringTokenizer(url, ":");
					synefoIP = strTok.nextToken();
					synefoPort = Integer.parseInt(strTok.nextToken());
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("INFO: Synefo started and listens at " + synefoIP + 
				":" + synefoPort + ".");
		
		String[] topologyCommand = {
				StormTopologyConf.stormCommand,
				StormTopologyConf.topologyJar,
				StormTopologyConf.topologyClass,
				synefoIP,
				synefoPort.toString(),
				remoteStreamSourceIP,
				remoteStreamSourcePort.toString()
		};
		BufferedReader topologyOutput = null;
		Process topology = null;
		ProcessBuilder topologyProcessBuilder = new ProcessBuilder(synefoCommand);
		topologyProcessBuilder.redirectErrorStream(true);
		try {
			topology = topologyProcessBuilder.start();
			topologyOutput = new BufferedReader(
					new InputStreamReader(topology.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
