package org.platform.jstorm.elasticsearch.read;

import java.util.HashMap;
import java.util.Map;

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
import org.elasticsearch.storm.EsSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ES2StormTopology {

	public static final String TOPOLOGY_NAME = "es-jstorm-topology";
	
	public static void main(String[] args) {
		if (args.length != 2) {
			System.exit(0);
		}
		String topologyName = args[0];
		boolean isCluster = Boolean.parseBoolean(args[1]);
		System.out.println("topology name: " + topologyName);
		
		TopologyBuilder builder = new TopologyBuilder();
		String target = "operator/telecom";
		String query = "?q=*";
		Map<Object, Object> configuration = new HashMap<Object, Object>();
		configuration.put("es.nodes", "192.168.0.114:9200");
		configuration.put("es.read.field.include", "name,phone,rcall,email,idCard,zipCode,address");
		configuration.put("es.storm.spout.fields", "name,phone,rcall,email,idCard,zipCode,address");
//		configuration.put("es.resource", "financial_new/business"); 
//		configuration.put("es.index.auto.create", "true");
		builder.setSpout("es-jstorm-spout", new EsSpout(target, query, configuration), 1);
		
		builder.setBolt("jstorm-handle-bolt", new HandleBolt()).shuffleGrouping("es-jstorm-spout");
		
		RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(":");
		SyncPolicy syncPolicy = new CountSyncPolicy(10);
		FileRotationPolicy fileRotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/jstorm/")
				.withPrefix("es_").withExtension(".log");
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://host-103:9000")
				.withFileNameFormat(fileNameFormat).withRecordFormat(recordFormat)
				.withRotationPolicy(fileRotationPolicy).withSyncPolicy(syncPolicy);
		builder.setBolt("jstorm-hdfs-bolt", hdfsBolt).globalGrouping("jstorm-handle-bolt");
		
		Config config = new Config();
		config.setDebug(true);
		if (isCluster) {
			try {
				config.setNumWorkers(1);
				StormSubmitter.submitTopologyWithProgressBar(
						topologyName, config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, config, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
		}
		
	}
	
}
