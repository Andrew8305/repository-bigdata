package org.platform.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IDGenerator {
	
	public static String generateByMapValues(Map<String, Object> map, String... excludeKeys) {
		List<String> excludeKeyList = Arrays.asList(excludeKeys);
		StringBuilder sb = new StringBuilder();
		String key = null;
		Object value = null;
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			key = entry.getKey();
			value = entry.getValue();
			if (excludeKeyList.contains(key)) continue;
			sb.append(value);
		}
		return MD5Utils.hash(sb.toString());
	}
	
}
