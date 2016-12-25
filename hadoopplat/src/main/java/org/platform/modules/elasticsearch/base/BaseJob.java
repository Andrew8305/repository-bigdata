package org.platform.modules.elasticsearch.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseJob extends Configured implements Tool {
	
	protected Logger LOG = LoggerFactory.getLogger(getClass());
	
	//成功
	public static final int SUCCESS = 0;
	//失败
	public static final int FAILURE = 1;

	/** 获取JOB名称*/
	protected String getJobName() {
        return getClass().getSimpleName();
	}

}
