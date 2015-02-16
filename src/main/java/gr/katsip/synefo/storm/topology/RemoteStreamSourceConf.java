package gr.katsip.synefo.storm.topology;

public class RemoteStreamSourceConf {
	
	public static final String jvm = "java -cp";
	
	public static final String remoteSourceJar = "streamgen.jar";

	public static String executionClass = "gr.katsip.streamgen.remotesource.RemoteStreamMain";
	
}
