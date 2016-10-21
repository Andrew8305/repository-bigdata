package org.platform.jstorm.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
import org.elasticsearch.storm.EsSpout;
import org.platform.jstorm.kafka.write.ESTupleToKafkaMapper;

import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class KafkaWriterTopology {
	
	private static final String TOPOLOGY_NAME = "es-jstorm-kafka-topology";
	
	private static final String E_J_SPOUT_NAME = "es-jstorm-spout";
	
	private static final String J_K_BOLT_NAME = "jstorm-kafka-bolt";
	
	private static final String J_H_BOLT_NAME = "jstorm-hdfs-bolt";
	
	private static final boolean isCluster = true;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		String target = "financial/logistics";
		String query = "?q=*";
		Map<Object, Object> spoutConf = new HashMap<Object, Object>();
		spoutConf.put("es.nodes", "192.168.0.114:9200");
		spoutConf.put("es.read.metadata", "true");
//		spoutConf.put("es.read.metadata.field", "_id,_score");
		builder.setSpout(E_J_SPOUT_NAME, new EsSpout(target, query, spoutConf));
		
		String topic = "kafka-elasticsearch-04";
		KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();
		kafkaBolt.withTopicSelector(new DefaultTopicSelector(topic))
			.withTupleToKafkaMapper(new ESTupleToKafkaMapper<String, String>());
		builder.setBolt(J_K_BOLT_NAME, kafkaBolt).globalGrouping(E_J_SPOUT_NAME);
		
		RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(":");
		SyncPolicy syncPolicy = new CountSyncPolicy(10);
//		FileRotationPolicy fileRotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
		FileRotationPolicy fileRotationPolicy = new FileSizeRotationPolicy(10, Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/jstorm/")
				.withPrefix("e_h_").withExtension(".log");
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://192.168.0.115:9000")
				.withFileNameFormat(fileNameFormat).withRecordFormat(recordFormat)
				.withRotationPolicy(fileRotationPolicy).withSyncPolicy(syncPolicy);
		
		builder.setBolt(J_H_BOLT_NAME, hdfsBolt).globalGrouping(E_J_SPOUT_NAME);
		
		Config config = new Config();
		config.setDebug(true);
		Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.0.115:9092");
        props.put("producer.type", "async");
        props.put("request.required.acks", "0"); // 0, -1, 1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        config.put(KafkaBolt.TOPIC, topic);
		
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
