package org.platform.jstorm.kafka.write;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@SuppressWarnings({ "rawtypes", "serial" })
public class ESTupleToKafkaMapper<K, V> implements TupleToKafkaMapper {
	
	private static final Logger LOG = LoggerFactory.getLogger(ESTupleToKafkaMapper.class);

	@SuppressWarnings("unchecked")
	@Override
	public K getKeyFromTuple(Tuple tuple) {
		if (tuple.contains("doc")) {
			LinkedHashMap<String, Object> doc = (LinkedHashMap<String, Object>) tuple.getValueByField("doc");
			Map<String, Object> metadata = (Map<String, Object>) doc.get("_metadata");
			return (K) metadata.get("_id");
		}
		return null;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public V getMessageFromTuple(Tuple tuple) {
		if (tuple.contains("doc")) {
			LinkedHashMap<String, Object> doc = (LinkedHashMap<String, Object>) tuple.getValueByField("doc");
			Map<String, Object> record = new HashMap<String, Object>();
			String key = null;
			for (Map.Entry<String, Object> entry : doc.entrySet()) {
				key = entry.getKey();
				if ("_metadata".equalsIgnoreCase(key)) {
					record.put("_id", ((Map<String, Object>) doc.get("_metadata")).get("_id"));
				} else {
					record.put(key, entry.getValue());
				}
			}
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			V message = (V) gson.toJson(record);
			LOG.info("message: " + message);
			return message;
		}
		return null;
	}

}
