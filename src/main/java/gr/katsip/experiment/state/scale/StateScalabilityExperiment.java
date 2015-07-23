package gr.katsip.experiment.state.scale;

import java.io.IOException;

public class StateScalabilityExperiment {

	public static void main(String[] args) throws IOException {
		String orderFile = "D:\\expanded-tpch\\expanded-orders-8.tbl";
		String lineFile = "D:\\expanded-tpch\\expanded-lineitem-8.tbl";
		StateScalabilityBenchmark benchmark = new StateScalabilityBenchmark(600000, 1000, orderFile, lineFile);
		benchmark.benchmark();
	}

}
