package org.platform.jstorm.elasticsearch.read;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HandleBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector = null;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String name = "NA";
		if (input.contains("name")) {
			name = input.getStringByField("name");
		}
		String phone = "NA";
		if (input.contains("phone")) {
			phone = input.getStringByField("phone");
		}
		String rcall = "NA";
		if (input.contains("rcall")) {
			rcall = input.getStringByField("rcall");
			rcall = null == rcall || "null".equals(rcall) ? "NA" : rcall;
		}
		String address = "NA";
		if (input.contains("address")) {
			address = input.getStringByField("address");
			address = null == address || "null".equals(address) ? "NA" : address;
		}
		String email = "NA";
		if (input.contains("email")) {
			email = input.getStringByField("email");
			email = null == email || "null".equals(email) ? "NA" : email;
		}
		String idCard = "NA";
		if (input.contains("idCard")) {
			idCard = input.getStringByField("idCard");
			idCard = null == idCard || "null".equals(idCard) ? "NA" : idCard;
		}
		this.collector.emit(new Values(name, phone, rcall, address, email, idCard));
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name", "phone", "rcal", "address", "email", "idCard"));
	}

}
