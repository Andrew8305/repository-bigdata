package org.platform.modules.elasticsearch.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BaseHDFS2ESV1Job extends BaseJob {
	
	/**
	 * 参数1：ES Index
	 * 参数2：ES Type
	 * 参数3：ES 集群名称
	 * 参数4：ES 集群IP
	 * 参数5：HDFS输入路径
	 * 参数6：HDFS输出路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("hadoop.job.user", "dataplat"); 
		conf.set("esIndex", args[0]);
		conf.set("esType", args[1]); 
		conf.set("esClusterName", args[2]); 
		conf.set("esClusterIP", args[3]); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 6) {
			LOG.error("error! need 6 input parameters!");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(BaseHDFS2ESV1Job.class);
		job.setMapperClass(BaseHDFS2ESV1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(BaseHDFS2ESV1Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 4; i < (args_len - 1); i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(oArgs[args_len - 1]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
