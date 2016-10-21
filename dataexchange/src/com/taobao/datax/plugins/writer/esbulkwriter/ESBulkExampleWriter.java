package com.taobao.datax.plugins.writer.esbulkwriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.ningmeng.es.commons.NingMengESUtils;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.Line;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.umeng.es.config.EsConfig;
import com.umeng.es.config.EsServerAddress;

public class ESBulkExampleWriter extends Writer {
	
	private String esIndex = null;
	
	private String esType = null;
	
	private List<String> pojos = null;

	private NingMengESUtils esUtils = null;

	private Logger logger = Logger.getLogger(ESBulkExampleWriter.class.getCanonicalName());

	@Override
	public int init() {
		this.esIndex = param.getValue(ParamKey.esIndex, "user");
		this.esType = param.getValue(ParamKey.esType, "student");
		this.pojos = new ArrayList<String>();
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int connect() {
		List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
		serverAddress.add(new EsServerAddress("192.168.0.105", 9300));
		serverAddress.add(new EsServerAddress("192.168.0.108", 9300));
		this.esUtils = new NingMengESUtils(new EsConfig("youmeng", serverAddress));
		return PluginStatus.SUCCESS.value();
	}
	
	@Override
	public int startWrite(LineReceiver receiver) {
		Line line = null;
		Student student = null;
		Gson gson = new Gson();
		try {
			while ((line = receiver.getFromReader()) != null) {
				if (null != line && line.getFieldNum() > 0) {
					student = new Student();
					String field1 = line.getField(0);
					if (StringUtils.isNotBlank(field1)) {
						student.setId(Integer.parseInt(field1));
					}
					String field2 = line.getField(1);
					if (StringUtils.isNotBlank(field2)) {
						student.setUserid(Long.parseLong(field2));
					}
					String field3 = line.getField(2);
					if (StringUtils.isNotBlank(field3)) {
						student.setName(field3);
					}
					String field4 = line.getField(3);
					if (StringUtils.isNotBlank(field4)) {
						student.setPhone(field4);
					}
					pojos.add(gson.toJson(student));
				}
				
			}
			return PluginStatus.SUCCESS.value();
		}  catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e.getCause());
		}
	}

	@Override
	public int commit() {
		esUtils.bulkSaveOrUpdate(pojos, esIndex, esType);
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int finish() {
		return 0;
	}
	
}
