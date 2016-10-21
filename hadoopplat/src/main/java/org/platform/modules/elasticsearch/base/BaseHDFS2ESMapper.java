package org.platform.modules.elasticsearch.base;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BaseHDFS2ESMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ESMapper.class);
	
	private TransportClient client = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private Gson gson = null;
	
	private int batchSize = 1000;
	
	private List<Map<String, Object>> records = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.esIndex = (String) context.getConfiguration().get("esIndex");
		this.esType = (String) context.getConfiguration().get("esType");
		this.gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.records = new ArrayList<Map<String, Object>>(this.batchSize);
		String esClusterName = (String) context.getConfiguration().get("esClusterName");
		String esClusterIP = (String) context.getConfiguration().get("esClusterIP");
		Settings settings = Settings.builder().put("cluster.name", esClusterName)
				.put("client.tansport.sniff", true).build();
		client = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
		String[] esClusterIPs = esClusterIP.contains(",") ? 
				esClusterIP.split(",") : new String[]{esClusterIP};
		for (int i = 0, len = esClusterIPs.length; i < len; i++) {
			serverAddress.add(new EsServerAddress(esClusterIPs[i], 9301));
		}
		for (EsServerAddress address : serverAddress) {
			client.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, Object> record = gson.fromJson(value.toString(), Map.class);
		records.add(record);
		if (records.size() > this.batchSize) {
			bulkInsertIndexTypeDatas(records);
			records.clear();
		}
	}
	
	private void bulkInsertIndexTypeDatas(List<Map<String, Object>> datas) {
		if (null == datas || datas.isEmpty()) return;
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = datas.size(); i < len; i++) {
			source = datas.get(i);
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
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
		if (!records.isEmpty()) {
			bulkInsertIndexTypeDatas(records);
			records.clear();
		}
	}
	
}

class EsServerAddress implements Serializable {

	private static final long serialVersionUID = 1L;

	private String host = null;
	private Integer port = 9300;

	public EsServerAddress() {
		super();
	}

	public EsServerAddress(String host) {
		super();
		this.host = host;
	}

	public EsServerAddress(String host, Integer port) {
		super();
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((port == null) ? 0 : port.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EsServerAddress other = (EsServerAddress) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port == null) {
			if (other.port != null)
				return false;
		} else if (!port.equals(other.port))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EsServerAddress [host=");
		builder.append(host);
		builder.append(", port=");
		builder.append(port);
		builder.append("]");
		return builder.toString();
	}

}
