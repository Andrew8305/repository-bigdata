package org.platform.jstorm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SENTENCE_SPLIT_BOLT_ID = "sentence-split-bolt";
	private static final String WORD_COUNT_BOLT_ID = "word-count-bolt";
	private static final String WORD_REPORT_BOLT_ID = "word-report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	private static final boolean isCluster = false;
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout());
		builder.setBolt(SENTENCE_SPLIT_BOLT_ID, new SentenceSplitBolt(), 2)
					.setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(WORD_COUNT_BOLT_ID, new WordCountBolt(), 2)
					.setNumTasks(4).fieldsGrouping(SENTENCE_SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(WORD_REPORT_BOLT_ID, new WordReportBolt())
					.globalGrouping(WORD_COUNT_BOLT_ID);
		
		Config config = new Config();
		config.setDebug(true);
		
		if (isCluster) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(
					TOPOLOGY_NAME, config, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		}
	}

}
