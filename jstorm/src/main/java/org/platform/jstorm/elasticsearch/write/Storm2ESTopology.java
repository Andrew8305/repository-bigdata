package org.platform.jstorm.elasticsearch.write;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.storm.EsBolt;
import org.elasticsearch.storm.EsSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Storm2ESTopology {

	public static final String TOPOLOGY_NAME = "jstorm-es-topology";
	
	public static void main(String[] args) {
		if (args.length != 2) {
			System.exit(0);
		}
		String topologyName = args[0];
		boolean isCluster = Boolean.parseBoolean(args[1]);
		
		System.out.println("topologyName: " + topologyName);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		String target = "financial/logistics";
		String query = "?q=*";
		Map<Object, Object> spoutConf = new HashMap<Object, Object>();
		spoutConf.put("es.nodes", "192.168.0.114:9200");
//		spoutConf.put("es.read.field.include", "name,phone,rcall,email,idCard,zipCode,address");
//		spoutConf.put("es.storm.spout.fields", "name,phone,rcall,email,idCard,zipCode,address");
		builder.setSpout("es-storm-spout", new EsSpout(target, query, spoutConf), 1);
		
//		builder.setSpout("json-storm-spout", new SentenceSpout());
		
		Map<Object, Object> boltConf = new HashMap<Object, Object>();
		boltConf.put("es.nodes", "192.168.0.114:9200");
		boltConf.put("es.index.auto.create", "false");
		boltConf.put("es.input.json", "true");
		builder.setBolt("storm-es-bolt", new EsBolt("data/logistics", boltConf))
			.globalGrouping("es-storm-spout");
		
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
