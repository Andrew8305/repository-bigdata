package org.platform.modules.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnJobMonitor {
	
	private static Logger LOG = LoggerFactory.getLogger(YarnJobMonitor.class);

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.tracker", "host-30:9001");
		conf.set("yarn.resourcemanager.address", "host-30:8032");
		conf.set("yarn.resourcemanager.admin.address", "host-30:8033");
		conf.set("yarn.resourcemanager.webapp.address", "host-30:8088");
		conf.set("yarn.resourcemanager.scheduler.address", "host-30:8030");
		conf.set("yarn.resourcemanager.resource-tracker.address", "host-30:8031");
		YARNRunner yarnRunner = new YARNRunner(conf);
		try {
			JobStatus[] jobStaties = yarnRunner.getAllJobs();
			for (JobStatus jobStatus : jobStaties) {
				LOG.info("job id: {}", jobStatus.getJobID());
				LOG.info("job name: {}", jobStatus.getJobName());
				LOG.info("job user: {}", jobStatus.getUsername());
				LOG.info("job queue: {}", jobStatus.getQueue());
				LOG.info("job starttime: {}", jobStatus.getStartTime());
				LOG.info("job finishtime: {}", jobStatus.getFinishTime());
				LOG.info("job state: {}", jobStatus.getState().name());
				LOG.info("job file: {}", jobStatus.getJobFile());
				LOG.info("job map progress: {}", jobStatus.getMapProgress());
				LOG.info("job reduce progress: {}", jobStatus.getReduceProgress());
				LOG.info("job scheduling info: {}", jobStatus.getSchedulingInfo());
				LOG.info("job used memory: {}", jobStatus.getUsedMem());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
}
