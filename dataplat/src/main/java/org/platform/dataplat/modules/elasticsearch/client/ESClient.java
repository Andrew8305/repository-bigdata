package org.platform.dataplat.modules.elasticsearch.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClient {
	
	/** 备份107库*/
	private TransportClient client20 = null;
	/** 备份102库*/
	private TransportClient client21 = null;
	/** 正式105-108库*/
	private TransportClient client105 = null;
	/** 测试114库*/
	private TransportClient client114 = null;
	
	private ESClient() {
		initClient20();
		initClient21();
		initClient105();
		initClient114();
	}
	
	private static class ESClientHolder {
		private static final ESClient INSTANCE = new ESClient();
	}
	
	public static final ESClient getInstance() {
		return ESClientHolder.INSTANCE;
	}
	
	public Client getClient() {
		return getClient114();
	}
	
	public Client getClient21() {
		if (null == client21) initClient21();
		return client21;
	}

	public Client getClient105() {
		if (null == client105) initClient105();
		return client105;
	}
	
	public Client getClient20() {
		if (null == client20) initClient20();
		return client20;
	}
	
	public Client getClient114() {
		if (null == client114) initClient114();
		return client114;
	}
	
	public void closeClient(Client client) {
		client.close();
	}
	
	private void initClient21() {
		Settings settings = Settings.builder().put("cluster.name", "cisiondata")
				.put("client.tansport.sniff", true).build();
		client21 = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> esServerAddress = new ArrayList<EsServerAddress>();
		esServerAddress.add(new EsServerAddress("192.168.0.21", 9300));
		for (EsServerAddress address : esServerAddress) {
			client21.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
	
	private void initClient105() {
		Settings settings = Settings.builder().put("cluster.name", "youmeng")
				.put("client.tansport.sniff", true).build();
		client105 = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> esServerAddress = new ArrayList<EsServerAddress>();
		esServerAddress.add(new EsServerAddress("192.168.0.105", 9300));
		esServerAddress.add(new EsServerAddress("192.168.0.108", 9300));
		for (EsServerAddress address : esServerAddress) {
			client105.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
	
	private void initClient20() {
		Settings settings = Settings.builder().put("cluster.name", "youmeng")
				.put("client.tansport.sniff", true).build();
		client20 = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> esServerAddress = new ArrayList<EsServerAddress>();
		esServerAddress.add(new EsServerAddress("192.168.0.20", 9300));
		for (EsServerAddress address : esServerAddress) {
			client20.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
	
	private void initClient114() {
		Settings testSettings = Settings.builder().put("cluster.name", "youmeng")
				.put("client.tansport.sniff", true).build();
		client114 = TransportClient.builder().settings(testSettings).build();
		List<EsServerAddress> testServerAddress = new ArrayList<EsServerAddress>();
		testServerAddress.add(new EsServerAddress("192.168.0.114", 9300));
		for (EsServerAddress address : testServerAddress) {
			client114.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}

}
