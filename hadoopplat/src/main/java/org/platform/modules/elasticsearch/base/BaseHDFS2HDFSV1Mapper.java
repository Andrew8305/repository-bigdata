package org.platform.modules.elasticsearch.base;

import java.util.Map;

/**
 * 主要处理数据格式为JSON的输入文件
 */
public abstract class BaseHDFS2HDFSV1Mapper extends BaseHDFS2HDFSMapper {
	
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> extractInputRecord(String inputRecord) {
		return gson.fromJson(inputRecord, Map.class);
	}
	
}
