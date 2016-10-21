package org.platform.jstorm.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceSplitBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (int i = 0, len = words.length; i < len; i++) {
			this.collector.emit(new Values(words[i]));
		}
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
