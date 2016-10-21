package org.platform.modules.elasticsearch.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public abstract class BaseHDFS2HDFSJob extends BaseJob {
	
	/**
	 * 获取Mapper类
	 * @return
	 */
	public abstract Class<? extends BaseHDFS2HDFSMapper> getMapperClass();
	
	/**
	 * 参数1：HDFS输入路径
	 * 参数2：HDFS输出路径
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("hadoop.job.user", "dataplat"); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length != 2) {
			LOG.error("args error! need input path and output path");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(getClass());
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(oArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(oArgs[1]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
