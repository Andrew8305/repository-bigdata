package org.platform.dataplat.modules.mapreduce.abstr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BaseHDFS2ESJob extends BaseJob {
	
	/**
	 * 参数1：ES Index
	 * 参数2：ES Type
	 * 参数3：ES 集群名称
	 * 参数4：ES 集群IP
	 * 参数5：HDFS输入路径
	 */
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
		if (oArgs.length != 5) {
			LOG.error("error");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "BaseHDFS2ESJob");
		job.setJarByClass(BaseHDFS2ESJob.class);
		job.setMapperClass(BaseHDFS2ESMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(oArgs[4]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
