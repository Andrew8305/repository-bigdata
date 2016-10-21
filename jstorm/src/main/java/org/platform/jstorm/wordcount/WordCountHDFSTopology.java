package org.platform.jstorm.wordcount;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountHDFSTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SENTENCE_SPLIT_BOLT_ID = "sentence-split-bolt";
	private static final String WORD_COUNT_BOLT_ID = "word-count-bolt";
	private static final String WORD_HDFS_BOLT_ID = "word-hdfs-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	private static final boolean isCluster = false;
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout());
		builder.setBolt(SENTENCE_SPLIT_BOLT_ID, new SentenceSplitBolt(), 2)
					.setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(WORD_COUNT_BOLT_ID, new WordCountBolt(), 2)
					.setNumTasks(4).fieldsGrouping(SENTENCE_SPLIT_BOLT_ID, new Fields("word"));
		
		RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(":");
		SyncPolicy syncPolicy = new CountSyncPolicy(10);
		FileRotationPolicy fileRotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/jstorm/")
				.withPrefix("wc_").withExtension(".log");
		
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://host-107:9000")
				.withFileNameFormat(fileNameFormat).withRecordFormat(recordFormat)
				.withRotationPolicy(fileRotationPolicy).withSyncPolicy(syncPolicy);
		
		
		builder.setBolt(WORD_HDFS_BOLT_ID, hdfsBolt).globalGrouping(WORD_COUNT_BOLT_ID);
		
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
