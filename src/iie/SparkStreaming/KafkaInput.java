package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaInput implements StreamingOperator {

	@Override
	public HashMap<String, DStreamWithSchema> execute(String ssc, String arguments,
			HashMap<String, DStreamWithSchema> record) {
		System.out.println("KafKaInput.execute finished.");
		for(Entry kv:record.entrySet()){
			System.out.print("input port value is:"+kv.getKey()+"--"+kv.getValue()+"\n\n\n");
		}
		HashMap<String, DStreamWithSchema> map = new HashMap<String, DStreamWithSchema>();
		map.put("kafkaInput.outport1", null);
		return map;
	}

}
