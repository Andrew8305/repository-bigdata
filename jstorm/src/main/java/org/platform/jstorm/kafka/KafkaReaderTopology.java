package org.platform.jstorm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.platform.jstorm.kafka.read.ESBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class KafkaReaderTopology {
	
	private static final String TOPOLOGY_NAME = "kafka-jstorm-es-topology";
	
	private static final String K_J_SPOUT_NAME = "kafka-jstorm-spout";
	
	private static final String J_E_BOLT_NAME = "jstorm-es-bolt";
	
	private static final String K_H_BOLT_NAME = "kafka-hdfs-bolt";
	
	private static final boolean isCluster = true;

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		String brokerZks = "192.168.0.115:2181";
		String topic = "kafka-elasticsearch-04";
		String zkRoot = "/kafka";
		String id = "data";
		BrokerHosts brokerHosts = new ZkHosts(brokerZks, "/kafka/brokers");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		List<String> zkServers = new ArrayList<String>();
		zkServers.add("192.168.0.115");
		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = 2181;
		builder.setSpout(K_J_SPOUT_NAME, new KafkaSpout(spoutConfig), 1);
		
//		Map<Object, Object> boltConf = new HashMap<Object, Object>();
//		boltConf.put("es.nodes", "192.168.0.114:9200");
//		boltConf.put("es.index.auto.create", "false");
//		boltConf.put("es.input.json", "true");
//		builder.setBolt(J_E_BOLT_NAME, new EsBolt("data/logistics", boltConf))
//			.globalGrouping(K_J_SPOUT_NAME);
		
		Map<String, Object> boltConf = new HashMap<String, Object>();
		boltConf.put("esClusterName", "youmeng");
		boltConf.put("esClusterIP", "192.168.0.114");
		boltConf.put("esIndex", "data");
		boltConf.put("esType", "logistics");
		builder.setBolt(J_E_BOLT_NAME, new ESBolt(boltConf))
			.globalGrouping(K_J_SPOUT_NAME);
		
		RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(":");
		SyncPolicy syncPolicy = new CountSyncPolicy(10);
//		FileRotationPolicy fileRotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
		FileRotationPolicy fileRotationPolicy = new FileSizeRotationPolicy(10, Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/jstorm/")
				.withPrefix("k_h_").withExtension(".log");
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://192.168.0.115:9000")
				.withFileNameFormat(fileNameFormat).withRecordFormat(recordFormat)
				.withRotationPolicy(fileRotationPolicy).withSyncPolicy(syncPolicy);
		
		builder.setBolt(K_H_BOLT_NAME, hdfsBolt).globalGrouping(K_J_SPOUT_NAME);
		
		Config config = new Config();
		config.setDebug(true);
		
		if (isCluster) {
			try {
				config.setNumWorkers(1);
				StormSubmitter.submitTopologyWithProgressBar(
						TOPOLOGY_NAME, config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		}
	}
	
}
