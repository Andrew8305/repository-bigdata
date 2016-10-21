package com.taobao.datax.plugins.writer.esbulkwriter;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class ESBulkWithoutIDWriter extends Writer {
	
	private String esIndex = null;
	
	private String esType = null;
	
	private String attrNameString = null;
	
	private String attrNameSplit = null;
	
	private String[] attrNames = null;
	
	private String className = null;
	
	private List<String> pojos = null;

	private NingMengESUtils esUtils = null;

	private Logger logger = Logger.getLogger(ESBulkWithoutIDWriter.class.getCanonicalName());

	@Override
	public int init() {
		this.esIndex = param.getValue(ParamKey.esIndex, "user");
		this.esType = param.getValue(ParamKey.esType, "student");
		this.attrNameString = param.getValue(ParamKey.attrNameString, "name,phone");
		this.attrNameSplit = param.getValue(ParamKey.attrNameSplit, ",");
		attrNames = attrNameString.split(attrNameSplit);
		this.className = param.getValue(ParamKey.className);
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
		Map<String, String> attrValueMap = null;
		Gson gson = new Gson();
		try {
			Object object = Class.forName(className).newInstance();
			while ((line = receiver.getFromReader()) != null) {
				int fieldNum = line.getFieldNum();
				if (null != line && fieldNum > 0) {
					attrValueMap = new HashMap<String, String>();
					for (int i = 0; i < fieldNum; i++) {
						attrValueMap.put(attrNames[i].toLowerCase(), line.getField(i));
					}
					for (Class<?> superClass = object.getClass(); 
							superClass != Object.class; superClass = superClass.getSuperclass()) {
			        	Field[] fields = superClass.getDeclaredFields();
			    		for (int i = 0, len = fields.length; i < len; i++) {
							Field field = fields[i];
							String fieldNameLowerCase = field.getName().toLowerCase();
							if (!attrValueMap.containsKey(fieldNameLowerCase)) continue;
							String valueString = attrValueMap.get(fieldNameLowerCase);
							Object value = convertValueByFieldType(field.getType(), valueString);
							if (field.isAccessible()) {
					            field.set(object, value);
					        } else {
					            field.setAccessible(true);
					            field.set(object, value);
					            field.setAccessible(false);
					        }
			    		}
			        }
					pojos.add(gson.toJson(object));
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
		int pojosSize = pojos.size();
		logger.info("pojos size : " + pojosSize);
		int fromIndex = 0, toIndex = 0;
		for (int i = 0, len = pojosSize / 1000; i <= len; i++) {
			fromIndex = i * 1000;
			toIndex = i < len ? fromIndex + 1000 : pojosSize;
			esUtils.bulkSaveOrUpdate(pojos.subList(fromIndex, toIndex), esIndex, esType);
		}
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int finish() {
		return 0;
	}
	
	private Object convertValueByFieldType(Class<?> type, Object value) {
    	Object finalValue = value;
    	if (String.class.isAssignableFrom(type)) {
    		finalValue = String.valueOf(value);
		} else if (Boolean.class.isAssignableFrom(type)) {
    		finalValue = Boolean.parseBoolean(String.valueOf(value));
		} else if (Integer.class.isAssignableFrom(type)) {
    		finalValue = Integer.parseInt(String.valueOf(value));
		} else if (Long.class.isAssignableFrom(type)) {
			finalValue = Long.parseLong(String.valueOf(value));
		} else if (Float.class.isAssignableFrom(type)) {
			finalValue = Float.parseFloat(String.valueOf(value));
		} else if (Double.class.isAssignableFrom(type)) {
			finalValue = Double.parseDouble(String.valueOf(value));
		} else if (Date.class.isAssignableFrom(type)) {
			try {
				finalValue = DateFormat.TIME.get().parse(String.valueOf(value));
			} catch (ParseException e) {
				logger.error(e.getMessage(), e);
			}
		} 
    	return finalValue;
    }
	
}
