package iie.SparkStreaming;

import java.util.HashMap;
import java.util.Map.Entry;

public class KafkaOutput implements StreamingOperator {

	@Override
	public HashMap<String, String> execute(String ssc, String arguments,
			HashMap<String, String> record) {
		System.out.println("KafkaOutput.execute finished.");
		for(Entry kv:record.entrySet()){
			System.out.print("input port value is:"+kv.getKey()+"--"+kv.getValue()+"\n\n\n");
		}
		HashMap<String, String> map = new HashMap<String, String>();
		return map;
	}

}
