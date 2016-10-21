package com.taobao.datax.plugins.writer.esbulkwriter;

import java.util.UUID;

public class ESIDGenerator {

	public static String UUID() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}
	
}
