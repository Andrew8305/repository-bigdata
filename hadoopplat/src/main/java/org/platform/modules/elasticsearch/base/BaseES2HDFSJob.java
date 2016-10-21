package org.platform.modules.elasticsearch.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsInputFormat;

public class BaseES2HDFSJob extends BaseJob {
	
	/**
	 * 参数1：ES节点IP
	 * 参数2：ES节点Index/Type
	 * 参数3：HDFS输出路径
	 * 参数3：HDFS输出文件记录分割条数
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			LOG.error("parameters must be four!");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("record.split.num", args[3]);
		conf.set("es.nodes", args[0] + ":9200");
		conf.set("es.resource", args[1]); 
		conf.set("es.read.metadata", "true"); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(BaseES2HDFSJob.class);
		job.setInputFormatClass(EsInputFormat.class);
		job.setMapperClass(BaseES2HDFSMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(oArgs[2]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

	public static void main(String args[]) {
		try {
			int exitCode = ToolRunner.run(new BaseES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

