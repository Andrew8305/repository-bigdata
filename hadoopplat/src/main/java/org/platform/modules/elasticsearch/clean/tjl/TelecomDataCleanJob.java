package org.platform.modules.elasticsearch.clean.tjl;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSMapper;
import org.platform.modules.elasticsearch.clean.util.CleanUtil;

public class TelecomDataCleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return TelecomHDFS2HDFSMapper.class;
	}
	
	
	public static void main(String[] args) {
		try {
			long starTime = System.currentTimeMillis();
			String n;
			int exitCode = 0;
			for (int j=5 ;j<=9;j++){
				for (int i = 0 ; i<=19 ; i++){
					if (i<10){
						n = "0"+i;
					} else {
						n = "" + i;
					}
//				args = new String[]{"hdfs://192.168.0.115:9000/elasticsearch/tjl/ot/records-"+index+"-m-000"+n,
//				"hdfs://192.168.0.115:9000/elasticsearch/tjl/ot_out/records-"+index+"-m-000"+n};
					args = new String[]{"hdfs://192.168.0.30:9000/elasticsearch/operator/telecom/21/records-"+j+"-m-000"+n,
							"hdfs://192.168.0.30:9000/elasticsearch_clean/operator/telecom/21/records-"+j+"-m-000"+n}; 
					System.out.println(args[0]+":"+args[1]);
					exitCode = ToolRunner.run(new TelecomDataCleanJob(), args);
				}
			}
			long endTime = System.currentTimeMillis();
			long times = endTime - starTime;
			System.out.println("spendTime:"+times/1000+"S");
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class TelecomHDFS2HDFSMapper extends BaseHDFS2HDFSMapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
			Map<String, Object> incorrect) {
		original = CleanUtil.replaceSpace(original);
		
		int size = 5;
		boolean flag = false;
		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			if (CleanUtil.matchIdCard(idCard)){
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()){
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
						|| entry.getKey().equals("idNumber"))continue;
					if (CleanUtil.matchIdCard((String)(entry.getValue()))){
						Object correctIdCard = entry.getValue();
						Object incorrectIdCard = original.get("idCard");
						entry.setValue(incorrectIdCard);
						original.put("idCard", correctIdCard);
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if(!cleanFlag)size++;
			}
		}
		if (original.containsKey("idNumber")) {
			String idNumber = (String) original.get("idNumber");
			if (CleanUtil.matchIdCard(idNumber)){
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()){
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
						|| entry.getKey().equals("idCard"))continue;
					if (CleanUtil.matchIdCard((String)(entry.getValue()))){
						Object correctIdNumber = entry.getValue();
						Object incorrectIdNumber = original.get("idNumber");
						entry.setValue(incorrectIdNumber);
						original.put("idNumber", correctIdNumber);
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if(!cleanFlag)size++;
			}
		}
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (CleanUtil.matchPhone(phone)){
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()){
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
						|| entry.getKey().equals("idCard") || entry.getKey().equals("rcall")
						|| entry.getKey().equals("idNumber"))continue;
					if (CleanUtil.matchPhone((String)(entry.getValue()))){
						Object correctPhone = entry.getValue();
						Object incorrectPhone = original.get("phone");
						original.put("phone", correctPhone);
						entry.setValue(incorrectPhone);
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag)size++;
			}
		}
		if (original.containsKey("rcall")){
			String rcall = (String)original.get("rcall");
			if (CleanUtil.matchPhone(rcall)){
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()){
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
						|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
						|| entry.getKey().equals("idNumber"))continue;
					if (CleanUtil.matchPhone((String)(entry.getValue()))){
						Object correctRcall = entry.getValue();
						Object incorrectRcall = original.get("rcall");
						entry.setValue(incorrectRcall);
						original.put("rcall", correctRcall);
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag)size++;
			}
		}
		if (original.size() <= size){
			flag = false;
		}
		
		if (flag){
			correct.putAll(original);
		} else {
			incorrect.putAll(original);
		}
	}
	
	
}
