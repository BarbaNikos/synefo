package gr.katsip.deprecated.deprecated;

import gr.katsip.deprecated.TopologyXMLParser.SynEFOTopologyBuilder;

/**
 * @deprecated
 */
public class SynEFOXMLTopology {

	public static void main(String[] args) throws Exception {
		String topologyFile = null;
		if(args.length < 1) {
			System.err.println("Arguments: <synEFO XML topology file>");
			System.exit(1);
		}else {
			topologyFile = args[0];
		}
		
		SynEFOTopologyBuilder builder = new SynEFOTopologyBuilder();
		builder.build(topologyFile);
		builder.submit();
	}

}
