package gr.katsip.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class RelationFileExpansion {
	
	private static float multiplier = -1;
	
	private static File file = null;
	
	private static long fileSize = -1;
	
	private static ArrayList<String> lines = null;

	public static void main(String[] args) throws IOException {
		if(args.length < 3) {
			System.err.println("Arguments needed: <file-name.txt> <output-file.txt> <size-multiplier>");
			System.exit(1);
		}else {
			file = new File(args[0]);
			multiplier = Float.parseFloat(args[2]);
			lines = new ArrayList<>();
		}
		if(!file.exists()) {
			System.err.println("input file is un-readable");
			System.exit(2);
		}
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = reader.readLine()) != null) {
			lines.add(line);
			fileSize += 1;
		}
		reader.close();
		File newFile = new File(args[1]);
		if(newFile.exists())
			newFile.delete();
		newFile.createNewFile();
		PrintWriter writer = new PrintWriter(new FileWriter(newFile));
		long newFileSize = (long) Math.ceil(fileSize * multiplier);
		int lineCount = 0;
		int listIndex = 0;
		while(lineCount < newFileSize) {
			writer.println(lines.get(listIndex));
			lineCount += 1;
			if(listIndex >= (lines.size() - 1))
				listIndex = 0;
			else
				listIndex += 1;
		}
		writer.close();
	}

}
