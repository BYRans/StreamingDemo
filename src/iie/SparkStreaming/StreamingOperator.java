package iie.SparkStreaming;

import java.util.HashMap;

public interface StreamingOperator {
	public HashMap<String, String> execute(String ssc, String arguments,
			HashMap<String, String> record);
}
