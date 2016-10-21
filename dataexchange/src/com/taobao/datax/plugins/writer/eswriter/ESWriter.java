package com.taobao.datax.plugins.writer.eswriter;

import org.apache.log4j.Logger;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.Line;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.plugins.writer.eswriter.ParamKey;

public class ESWriter extends Writer {
	
	private String singleCurl = "";
	
	private String nullString = "";
	
	private String columnNameString = "";
	
	private String columnNameSplit = "";
	
	private String[] columnNames = null;

	private Logger logger = Logger.getLogger(ESWriter.class.getCanonicalName());

	@Override
	public int init() {
		this.singleCurl = param.getValue(ParamKey.singleCurl, 
				"curl -XPOST 'http://192.168.0.108:9200/user/student/{id}' -d '{data}'");
		this.nullString = param.getValue(ParamKey.nullChar, this.nullString);
		this.columnNameString = param.getValue(ParamKey.columnNames, "userid,name,phone");
		this.columnNameSplit = param.getValue(ParamKey.columnNameSplit, ",");
		this.columnNames = columnNameString.split(columnNameSplit);
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int connect() {
		return PluginStatus.SUCCESS.value();
	}
	
	private String makeCurl(Line line) {
		if (line == null || line.getFieldNum() == 0) {
			return this.singleCurl + "\n";
		}
		String item = null;
		int num = line.getFieldNum();
		this.singleCurl = this.singleCurl.replace("{id}", line.getField(0));
		StringBuilder sb = new StringBuilder().append("{");
		for (int i = 1; i < num; i++) {
			item = line.getField(i);
			sb.append("\"").append(columnNames[i-1]).append("\":\"");
			if (null == item) {
				sb.append(nullString);
			} else {
				sb.append(item);
			}
			sb.append("\",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}");
		return new StringBuilder(this.singleCurl).toString().replace("{data}", sb.toString());
	}

	@Override
	public int startWrite(LineReceiver receiver) {
		Line line;
		try {
			while ((line = receiver.getFromReader()) != null) {
				String curlCommand = makeCurl(line);
				String[] commands = new String[3];  
			    commands[0] = "/bin/sh";  
			    commands[1] = "-c";  
			    commands[2] = curlCommand;  
				Runtime.getRuntime().exec(commands);
			}
			return PluginStatus.SUCCESS.value();
		}  catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e.getCause());
		}
	}

	@Override
	public int commit() {
		return 0;
	}

	@Override
	public int finish() {
		return 0;
	}
	
}
