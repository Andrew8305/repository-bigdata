package org.platform.dataplat.modules.mapreduce.abstr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseJob extends Configured implements Tool {
	
	protected Logger LOG = LoggerFactory.getLogger(getClass());
	
	//成功
	public static final int SUCCESS = 0;
	//失败
	public static final int FAILURE = 1;

	/** 获取JOB名称*/
	protected String getJobName() {
        return getClass().getSimpleName();
	}

	@Override
	public int run(String[] args) throws Exception {
		return ToolRunner.run(this, args);
	}
	
}
