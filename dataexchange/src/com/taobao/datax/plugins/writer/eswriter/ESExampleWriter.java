package com.taobao.datax.plugins.writer.eswriter;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.log4j.Logger;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.Line;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.engine.plugin.DefaultLine;
import com.taobao.datax.plugins.writer.streamwriter.ParamKey;

public class ESExampleWriter extends Writer {
	
	private char FIELD_SPLIT = '\t';

	private String ENCODING = "UTF-8";

	private String PREFIX = "";
	
	private boolean printable = true;
	
	private String nullString = "";
	
	private String[] columnNames = null;

	private Logger logger = Logger.getLogger(ESExampleWriter.class
			.getCanonicalName());

	@Override
	public int init() {
		this.FIELD_SPLIT = param.getCharValue(
				ParamKey.fieldSplit, '\t');
		this.ENCODING = param
				.getValue(ParamKey.encoding, "UTF-8");
		this.PREFIX = param.getValue(ParamKey.prefix, "");
		this.nullString = param.getValue(ParamKey.nullChar,
				this.nullString);
		this.printable = param.getBoolValue(ParamKey.print,
				this.printable);
		this.columnNames = new String[]{"userid", "name", "phone"};
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int connect() {
		return PluginStatus.SUCCESS.value();
	}
	
	private String makeCurl(Line line) {
		if (line == null || line.getFieldNum() == 0) {
			return this.PREFIX + "\n";
		}
		String item = null;
		int num = line.getFieldNum();
		
		this.PREFIX = "curl -XPOST 'http://192.168.0.108:9200/user/student/{id}' -d '{data}'";
		this.PREFIX = this.PREFIX.replace("{id}", line.getField(0));
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
		return new StringBuilder(this.PREFIX).toString().replace("{data}", sb.toString());
	}

	@Override
	public int startWrite(LineReceiver receiver) {
		Line line;
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					System.out, this.ENCODING));
			while ((line = receiver.getFromReader()) != null) {
				if (this.printable) {
					String curlCommand = makeCurl(line);
					writer.write(curlCommand + FIELD_SPLIT);
					String[] commands = new String[3];  
				    commands[0] = "/bin/sh";  
				    commands[1] = "-c";  
				    commands[2] = curlCommand;  
					Runtime.getRuntime().exec(commands);
				} else {
					/* do nothing */
				}
			}
			writer.flush();
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
	
	public static void main(String[] args) {
		Line line = new DefaultLine();
		line.addField("2");
		line.addField("372030730");
		line.addField("赵俊清");
		line.addField("15000593871");
		ESExampleWriter esWriter = new ESExampleWriter();
		System.out.println(esWriter.makeCurl(line));
	}

}
