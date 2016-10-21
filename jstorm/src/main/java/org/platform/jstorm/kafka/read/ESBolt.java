package org.platform.jstorm.kafka.read;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ESBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;

	public static final Logger LOG = LoggerFactory.getLogger(ESBolt.class);
	
	private transient OutputCollector collector = null;
	
	private TransportClient client = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private Gson gson = null;
	
	private int batchSize = 1000;

	private List<String> tuples = null;
	
	private Map<String, Object> conf = null;

	private long datasLength = 0;
	
	private long handleDatasLength = 0;
	
	public ESBolt(Map<String, Object> conf) {
		this.conf = conf;
		if (null == this.conf) {
			this.conf = new HashMap<String, Object>();
		}
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.esIndex = (String) this.conf.get("esIndex");
		this.esType = (String) this.conf.get("esType");
		this.tuples = new ArrayList<String>();
		this.gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		String esClusterName = (String) this.conf.get("esClusterName");
		String esClusterIP = (String) this.conf.get("esClusterIP");
		Settings settings = Settings.builder().put("cluster.name", esClusterName)
				.put("client.tansport.sniff", true).build();
		client = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
		String[] esClusterIPs = esClusterIP.contains(",") ? 
				esClusterIP.split(",") : new String[]{esClusterIP};
		for (int i = 0, len = esClusterIPs.length; i < len; i++) {
			serverAddress.add(new EsServerAddress(esClusterIPs[i], 9300));
		}
		for (EsServerAddress address : serverAddress) {
			client.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
		this.datasLength = readIndexTypeDatasLength();
	}
	
	private long readIndexTypeDatasLength() {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(this.esIndex).setTypes(this.esType);
		searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
		searchRequestBuilder.setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response.getHits().getTotalHits();
	}

	@Override
	public void execute(Tuple input) {
		this.handleDatasLength += 1;
		if (input.contains("str")) {
			tuples.add((String) input.getValueByField("str"));
		}
		if (tuples.size() == this.batchSize || this.handleDatasLength == this.datasLength) {
			LOG.info("handleDatasLength: " + this.handleDatasLength + " datasLength: " + this.datasLength);
			bulkInsertIndexTypeDatas(tuples);
			tuples.clear();
		}
		this.collector.ack(input);
	}
	
	@SuppressWarnings("unchecked")
	private void bulkInsertIndexTypeDatas(List<String> datas) {
		if (null == datas || datas.isEmpty()) return;
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = datas.size(); i < len; i++) {
			source = gson.fromJson(datas.get(i), Map.class);
			if (source.containsKey("_id")) {
				String _id = (String) source.remove("_id");
				irb = client.prepareIndex(this.esIndex, this.esType, _id);
				irb.setSource(source);
			} else {
				irb = client.prepareIndex(this.esIndex, this.esType).setSource(source);
			}
			bulkRequestBuilder.add(irb);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			LOG.info(bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public void cleanup() {
		if (!tuples.isEmpty()) {
			bulkInsertIndexTypeDatas(tuples);
		}
		client.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	

}
