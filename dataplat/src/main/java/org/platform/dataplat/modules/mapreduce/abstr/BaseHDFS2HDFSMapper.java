package org.platform.dataplat.modules.mapreduce.abstr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.platform.dataplat.utils.generator.IDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class BaseHDFS2HDFSMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private Logger LOG = LoggerFactory.getLogger(BaseHDFS2HDFSMapper.class);
	
	protected Gson gson = null;
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);//NullWritable 实现方法为空实现
		this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
			Map<String, Object> correct = new HashMap<String, Object>();
			Map<String, Object> incorrect = new HashMap<String, Object>();
			handle(original, correct, incorrect);
			if (!correct.isEmpty()) {
				String id = IDGenerator.generateByMapValues(correct, "insertTime",
                        "updateTime", "sourceFile");
				correct.put("_id", id);
				multipleOutputs.write(NullWritable.get(), new Text(gson.toJson(correct)), "correct");
			}
			if (!incorrect.isEmpty()) {
				multipleOutputs.write(NullWritable.get(), new Text(gson.toJson(incorrect)), "incorrect");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 处理原始数据，清理出正确数据与不正确数据
	 * @param original 原始数据
	 * @param correct 正确数据
	 * @param incorrect 不正确数据
	 */
	public abstract void handle(Map<String, Object> original, Map<String, Object> correct,
			Map<String, Object> incorrect);
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
	}

}
