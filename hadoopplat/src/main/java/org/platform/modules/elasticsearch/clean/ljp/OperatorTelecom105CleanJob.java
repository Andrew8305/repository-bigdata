package org.platform.modules.elasticsearch.clean.ljp;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSMapper;
import org.platform.modules.elasticsearch.clean.util.CleanUtil;

public class OperatorTelecom105CleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return TelecomHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			int exitCode = 0;		
			  int index; 
			  for(index=1;index<=10;index++){ 
				  for(int i=0;i<=9;i++){
			    args = new String[] {
			   "hdfs://192.168.0.30:9000/elasticsearch/operator/telecom/20/records-"
					  								+index+"-m-0000"+i,
			   "hdfs://192.168.0.30:9000/elasticsearch_clean/operator/telecom/20/records-"
					  								+index+"-m-0000"+i};
			  exitCode = ToolRunner.run(new OperatorTelecom105CleanJob(), args); 
			  }
		}
			 
		/*	args = new String[] { "hdfs://192.168.0.115:9000/elasticsearch/ljp/ot/test",
					"hdfs://192.168.0.115:9000/elasticsearch/ljp/ot_out/test" };*/
		//	exitCode = ToolRunner.run(new OperatorTelecom105CleanJob(), args);
			System.exit(exitCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class TelecomHDFS2HDFSMapper extends BaseHDFS2HDFSMapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {

		// 将所有的字段去空格
		CleanUtil.replaceSpace(original);

		int size = 5;
		boolean flag = false;
		boolean judge = true;
		/*
		 * idCard
		 */
		if (original.containsKey("idCard")) {
			String idCard = ((String) original.get("idCard"));
			if (CleanUtil.matchIdCard(idCard)) {
				flag = true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("sourceFile") || entry.getKey().equals("insertTime")
							|| entry.getKey().equals("idNumber")) {
						continue;
					}
					if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
						Object value = entry.getValue();
						Object idcard = original.get("idCard");
						original.put("idCard", value);
						entry.setValue(idcard);
						judge = false;
						flag = true;
						break;
					}
				}
				if (judge)
					size++;
			}
		}

		/*
		 * idNumber
		 */
		if (original.containsKey("idNumber")) {
			String idNumber = ((String) original.get("idNumber"));
			if (CleanUtil.matchIdCard(idNumber)) {
				flag = true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("sourceFile") || entry.getKey().equals("insertTime")
							|| entry.getKey().equals("idCard")) {
						continue;
					}
					if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
						Object value = entry.getValue();
						Object ToidNumber = original.get("idNumber");
						original.put("idNumber", value);
						entry.setValue(ToidNumber);
						judge = false;
						flag = true;
						break;
					}
				}
				if (judge)
					size++;
			}
		}

		/*
		 * phone
		 */
		if (original.containsKey("phone")) {
			String phone = ((String) original.get("phone"));
			if (CleanUtil.matchPhone(phone)) {
				flag = true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("sourceFile") || entry.getKey().equals("insertTime")
							|| entry.getKey().equals("idCard") || entry.equals("idNumber") || entry.equals("rcall")) {
						continue;
					}
					if (CleanUtil.matchPhone((String) (entry.getValue()))) {
						Object value = entry.getValue();
						Object Tophone = original.get("phone");
						original.put("phone", value);
						entry.setValue(Tophone);
						judge = false;
						flag = true;
						break;
					}
				}
				if (judge)
					size++;
			}
		}

		/*
		 * rCall
		 */
		if (original.containsKey("rcall")) {
			String rcall = ((String) original.get("rcall"));
			if (CleanUtil.matchPhone(rcall)) {
				flag = true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("sourceFile") || entry.getKey().equals("insertTime")
							|| entry.getKey().equals("idCard") || entry.equals("idNumber") || entry.equals("phone")) {
						continue;
					}
					if (CleanUtil.matchPhone((String) (entry.getValue()))) {
						Object value = entry.getValue();
						Object Torcall = original.get("rcall");
						original.put("rcall", value);
						entry.setValue(Torcall);
						judge = false;
						flag = true;
						break;
					}
				}
				if (judge)
					size++;
			}
		}

		// 字段数
		if (original.size() <= size) {
			flag = false;
		}

		// 放入结果集
		if (flag) {
			correct.putAll(original);
		} else {
			incorrect.putAll(original);

		}
	}
}
