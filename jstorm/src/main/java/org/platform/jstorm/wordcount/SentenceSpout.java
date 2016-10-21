package org.platform.jstorm.wordcount;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector = null;

	private String[] sentences = { "j2se j2ee j2me", "hibernate spring struts",
			"mybatis springmvc", "hadoop hbase hive pig", "spark mllib sql streming graph",
			"alluxio tachyon hdfs", "mongodb redis memcache cassandra" };

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void ack(Object messageId) {
		System.out.println("ack : " + messageId);
	}

	@Override
	public void activate() {
	}

	@Override
	public void close() {
		
	}

	@Override
	public void deactivate() {
		
	}

	@Override
	public void fail(Object messageId) {
		System.out.println("fail : " + messageId);
	}

	@Override
	public void nextTuple() {
		int index = new Random().nextInt(sentences.length);
		this.collector.emit(new Values(sentences[index]), sentences[index]);
		Utils.sleep(1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
