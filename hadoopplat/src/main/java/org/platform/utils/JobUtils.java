package org.platform.utils;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(JobUtils.class);
	
	public static YARNRunner getYarnRunner() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.tracker", "host-10:9001");
		conf.set("yarn.resourcemanager.address", "host-10:8032");
		conf.set("yarn.resourcemanager.admin.address", "host-10:8033");
		conf.set("yarn.resourcemanager.webapp.address", "host-10:8088");
		conf.set("yarn.resourcemanager.scheduler.address", "host-10:8030");
		conf.set("yarn.resourcemanager.resource-tracker.address", "host-10:8031");
		conf.set("mapreduce.jobhistory.address", "host-10:10020");
		return new YARNRunner(conf);
	}
	
	public static void MRJobMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				Counters counters = yarnRunner.getJobCounters(jobStatus.getJobID());
//				if (!jobStatus.getState().name().equalsIgnoreCase("FAILED")) continue;
				LOG.info("job id: {}", jobStatus.getJobID());
				LOG.info("job name: {}", jobStatus.getJobName());
//				LOG.info("job user: {}", jobStatus.getUsername());
//				LOG.info("job queue: {}", jobStatus.getQueue());
//				LOG.info("job starttime: {}", jobStatus.getStartTime());
//				LOG.info("job finishtime: {}", jobStatus.getFinishTime());
				LOG.info("job state: {}", jobStatus.getState().name());
				LOG.info("job file: {}", jobStatus.getJobFile());
				LOG.info("job map progress: {}", jobStatus.getMapProgress());
				LOG.info("job reduce progress: {}", jobStatus.getReduceProgress());
				LOG.info("job scheduling info: {}", jobStatus.getSchedulingInfo());
				LOG.info("job failure info: {}", jobStatus.getFailureInfo());
//				LOG.info("job used memory: {}", jobStatus.getUsedMem());
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void TaskTrackerMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			TaskTrackerInfo[] taskTrackerInfos = yarnRunner.getActiveTrackers();
			for (int i = 0, len = taskTrackerInfos.length; i < len; i++) {
				TaskTrackerInfo taskTrackerInfo = taskTrackerInfos[i];
				LOG.info("task tracker name: {}", taskTrackerInfo.getTaskTrackerName());
				LOG.info("task tracker report: {}", taskTrackerInfo.getBlacklistReport());
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void TaskMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				if (!"FinancialLogisticsHDFS2ESV2Job".equalsIgnoreCase(
						jobStatus.getJobName())) continue;
				
				if ("SUCCEEDED".equalsIgnoreCase(jobStatus.getState().name())) continue;
				
				LOG.info("job id : {} name: {}", jobStatus.getJobID(), jobStatus.getJobName());
				TaskReport[] mapTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.MAP);
				for (int j = 0, jLen = mapTaskReports.length; j < jLen; j++) {
					TaskReport taskReport = mapTaskReports[j];
//					if (!"KILLED".equalsIgnoreCase(taskReport.getState())) continue;
					LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
					for (String diagno : taskReport.getDiagnostics()) {
						LOG.info("msg: {}" + diagno);
					}
				}
				TaskReport[] reduceTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.REDUCE);
				for (int k = 0, kLen = reduceTaskReports.length; k < kLen; k++) {
					TaskReport taskReport = reduceTaskReports[k];
//					if (!"KILLED".equalsIgnoreCase(taskReport.getState())) continue;
					LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
				}
				System.out.println("######");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void CounterMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				LOG.info("job id: {}", jobStatus.getJobID());
				LOG.info("job name: {}", jobStatus.getJobName());
				Counters counters = yarnRunner.getJobCounters(jobStatus.getJobID());
				LOG.info("job counters: {}", counters.countCounters());
				Iterator<CounterGroup> iterator = counters.iterator();
				while (iterator.hasNext()) {
					CounterGroup counterGroup = iterator.next();
					LOG.info("counter group name: {}", counterGroup.getName());
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void JobStatistics() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			LOG.info("job history dir: {}", yarnRunner.getJobHistoryDir());
			String jobId = "job_1482820815021_4493";
			LogParams logParams = yarnRunner.getLogFileParams(JobID.forName(jobId), TaskAttemptID.forName("attempt_1482820815021_4493_m_000000_0"));
			LOG.info("log params owner: {}", logParams.getOwner());
			
			JobStatus jobStatus = yarnRunner.getJobStatus(JobID.forName(jobId));
			LOG.info("job file {}", jobStatus.getJobFile());
			LOG.info("job tracking url {}", jobStatus.getTrackingUrl());
			LOG.info("job schedule info {}", jobStatus.getSchedulingInfo());
			Counters jobCounters = yarnRunner.getJobCounters(JobID.forName(jobId));
			LOG.info("job counters {}", jobCounters);
			TaskReport[] mapTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.MAP);
			for (int j = 0, jLen = mapTaskReports.length; j < jLen; j++) {
				TaskReport taskReport = mapTaskReports[j];
				LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
				for (String diagno : taskReport.getDiagnostics()) {
//					LOG.info("msg: {}" + diagno);
				}
				Counters counters = taskReport.getTaskCounters();
				LOG.info("task counters: {}", counters);
				LOG.info("task total count: {}", counters.countCounters());
				Iterator<CounterGroup> iterator = counters.iterator();
				while (iterator.hasNext()) {
					CounterGroup counterGroup = iterator.next();
					LOG.info("counter group name: {}", counterGroup.getName());
					Iterator<Counter> counterIterator = counterGroup.iterator();
					while (counterIterator.hasNext()) {
						Counter counter = counterIterator.next();
//						LOG.info("counter name: {}", counter.getName());
					}
				}
			}
			
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}

	public static void main(String[] args) {
		JobStatistics();
	}
	
}
