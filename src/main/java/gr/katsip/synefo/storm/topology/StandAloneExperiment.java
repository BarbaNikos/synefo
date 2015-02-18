package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.utils.StormTopologyConf;
import gr.katsip.synefo.utils.SynefoConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class StandAloneExperiment {

	public static void main(String[] args) {
		String resourceConfFile = "";
		String zooIP = "";
		Integer zooPort = -1;
		String dataSourceFileName = "";
		
		if(args.length < 4) {
			System.err.println("Expected arguments: <resourceConfFile> <zoo-IP> <zoo-port> <input-data-file>");
			System.exit(1);
		}else {
			resourceConfFile = args[0];
			zooIP = args[1];
			zooPort = Integer.parseInt(args[2]);
			dataSourceFileName = args[3];
		}
		
		String remoteStreamSourceIP = "";
		Integer remoteStreamSourcePort = -1;
		String[] remoteSourceCommand = {
			RemoteStreamSourceConf.jvm, 
			RemoteStreamSourceConf.remoteSourceJar,
			RemoteStreamSourceConf.executionClass, 
			dataSourceFileName
		};
//		String[] remoteSourceCommand = {
//				"java",
//				"-jar"
//		};
//		System.out.print("Command: ");
//		for(String s : remoteSourceCommand) {
//			System.out.print(s + " ");
//		}
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
				remoteStreamSourcePort.toString(),
				zooIP,
				zooPort.toString()
		};
		@SuppressWarnings("unused")
		BufferedReader topologyOutput = null;
		Process topology = null;
		ProcessBuilder topologyProcessBuilder = new ProcessBuilder(topologyCommand);
		topologyProcessBuilder.redirectErrorStream(true);
		try {
			topology = topologyProcessBuilder.start();
			topologyOutput = new BufferedReader(
					new InputStreamReader(topology.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		/**
		 * Wait until the remoteSource ends to stop the experiment
		 * 
		 */
		try {
			remoteStreamSource.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("INFO: Remote source depleted the input. Time to end processes...");
		synefo.destroy();
		topology.destroy();
	}

}
