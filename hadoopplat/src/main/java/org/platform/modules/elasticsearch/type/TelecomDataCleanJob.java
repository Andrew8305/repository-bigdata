package org.platform.modules.elasticsearch.type;

import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSV1Mapper;

public class TelecomDataCleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return TelecomHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new TelecomDataCleanJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class TelecomHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
			Map<String, Object> incorrect) {
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (phone.startsWith("133")) {
				incorrect.putAll(original);
			} else {
				correct.putAll(original);
			}
		}
	}
	
	
}
