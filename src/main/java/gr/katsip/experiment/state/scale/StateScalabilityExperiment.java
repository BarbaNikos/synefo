package gr.katsip.experiment.state.scale;

import java.io.IOException;

public class StateScalabilityExperiment {

	public static void main(String[] args) throws IOException {
		String orderFile = "D:\\expanded-tpch\\expanded-orders-4.tbl";
		String lineFile = "D:\\expanded-tpch\\expanded-lineitem-4.tbl";
		StateScalabilityBenchmark benchmark = new StateScalabilityBenchmark(120000, 1000, orderFile, lineFile);
		benchmark.benchmark();
	}

}
