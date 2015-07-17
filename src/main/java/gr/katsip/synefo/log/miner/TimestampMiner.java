package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TimestampMiner {

	public static void main(String[] args) throws IOException {
		File directory = new File(args[0]);
		int falseCounter = 0;
		if(directory.isDirectory()) {
			File[] files = directory.listFiles();
			for(File logFile : files) {
				if(!logFile.isFile() || !logFile.getName().contains("-scale-events.log"))
					continue;
				String operator = logFile.getName().split("_")[0];
				BufferedReader reader = new BufferedReader(new FileReader(logFile));
				String line = "";
				while((line = reader.readLine()) != null) {
					String[] lineTokens = line.split(",");
					if(lineTokens.length >= 3) {
						Long time1 = Long.parseLong(lineTokens[0]);
						Long time2 = Long.parseLong(lineTokens[1]);
						if(Math.abs(time2 - time1) < 1000)
							falseCounter += 1;
					}
				}
				reader.close();
			}
		}
		System.out.println("False counter: " + falseCounter);
	}

}
