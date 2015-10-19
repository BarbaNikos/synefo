package gr.katsip.experiment.state.scale;

import java.io.IOException;

public class StateScalabilityExperiment {

	public static void main(String[] args) throws IOException {
		String orderFile = "D:\\expanded-tpch\\orders.tbl";
		String lineFile = "D:\\expanded-tpch\\lineitem.tbl";
		/**
		 * 15 minutes window and 1 second slide
		 */
		StateScalabilityBenchmark benchmark = new StateScalabilityBenchmark((15 * 60 * 1000), 1000, orderFile, lineFile);
		benchmark.benchmark();
	}

}
