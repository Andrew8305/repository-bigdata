package org.platform.jstorm.wordcount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordReportBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private HashMap<String, Long> counts = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counts.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	@Override
	public void cleanup() {
		System.out.println("--- FINAL WORD COUNT STATISTICS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.counts.get(key));
		}
		System.out.println("--------------");
	}

}
