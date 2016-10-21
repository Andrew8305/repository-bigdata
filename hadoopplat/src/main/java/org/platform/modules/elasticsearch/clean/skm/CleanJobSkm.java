package org.platform.modules.elasticsearch.clean.skm;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSMapper;
import org.platform.modules.elasticsearch.clean.lx.FinancialLogistics20CleanJob;
import org.platform.modules.elasticsearch.clean.util.CleanUtil;

public class CleanJobSkm extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return TelecomHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
					
	     	String fss ="hdfs://192.168.0.30:9000/elasticsearch/financial/logistics/105";
			Configuration conf = new Configuration();
			  FileSystem hdfs;
			try {
				hdfs = FileSystem.get(URI.create(fss),conf);
				FileStatus[] fs = hdfs.listStatus(new Path(fss));
				Path[] listPath =FileUtil.stat2Paths(fs);
				System.out.println("--------------------------------->"+listPath.length);
			
					
				int exitCode = 0;
				for(Path p : listPath){
					if(!p.getName().equals("_SUCCESS")&&!p.getName().equals("part-r-00000")){
						
						System.out.println(p.toString());
						System.out.println(p.getName());
						
						args = new String[]{p.toString(), 
								"hdfs://192.168.0.30:9000/elasticsearch_clean/financial/logistics/105/"+p.getName()};
						exitCode = ToolRunner.run(new FinancialLogistics20CleanJob(), args);
						
					} 	
				}		
				System.exit(exitCode);
				
		        
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	

}

class TelecomHDFS2HDFSMapper extends BaseHDFS2HDFSMapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		// 去除所以value值有空格的
		original = CleanUtil.replaceSpace(original);
		// 不为NA的有效字段个数
		int times = 0;
		// 姓名（存在一个汉字或字母及以上）
		String regName = "^.*([\u4e00-\u9fa5]|[a-zA-Z]){1,}.*$";
		// 至少存在一个汉字
		String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";
		Iterator<Map.Entry<String, Object>> maps = original.entrySet().iterator();
//		while (maps.hasNext()) {
//			Map.Entry<String, Object> entry = maps.next();
//			if (!"NA".equals(entry.getValue())) {
//				times++;
//			}
//		}
		int size = 5;
	    boolean flag=false;
	    boolean tager=true;
		// 判断身份证
				if (original.containsKey("idCard")) {
					if (CleanUtil.matchIdCard((String) original.get("idCard"))) {
						
						flag=true;
						
					} else {
						for (Entry<String, Object> entry : original.entrySet()) {
							if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
									|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
								continue;

							if (CleanUtil.matchPhone((String) entry.getValue())) {
								String m1 = (String) entry.getValue();
								String phone = (String) original.get("idCard");
								original.put("idCard", m1);
								entry.setValue(phone);
								
								flag=true;
								tager=false;
								break;
							}
						}
						
					}
					if(tager){
						size++;  
					}
				}
		
		
		// 判断phone
		if (original.containsKey("phone")) {
			if (CleanUtil.matchPhone((String) original.get("phone"))) {
				flag=true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("linkPhone") || entry.getKey().equals("linkCall")
							|| entry.getKey().equals("rcall")||entry.getKey().equals("idCard")) {
						continue;
					}
					if (CleanUtil.matchPhone((String) entry.getValue())) {
						String m1 = (String) entry.getValue();
						String phone = (String) original.get("phone");
						original.put("phone", m1);
						entry.setValue(phone);
						flag=true;
						tager=false;
						break;
					}
				}
				
			}
			if(tager){
				size++;
			}

		}
		
		// 判断rcall
				if (original.containsKey("rcall")) {
					if (CleanUtil.matchPhone((String) original.get("rcall"))) {
						
						flag=true;
						
					} else {
						for (Entry<String, Object> entry : original.entrySet()) {
							if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
									|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
									|| entry.getKey().equals("linkPhone") || entry.getKey().equals("linkCall")
									|| entry.getKey().equals("phone")||entry.getKey().equals("idCard")) {
								continue;
							}
							if (CleanUtil.matchPhone((String) entry.getValue())) {
								String m1 = (String) entry.getValue();
								String phone = (String) original.get("rcall");
								original.put("rcall", m1);
								entry.setValue(phone);
								flag=true;
								
								tager=false;
								break;
							}
						}
						
					}
					if(tager){
						size++;
					}
				}

		// 判断linkPhone
		if (original.containsKey("linkPhone")) {
			if (CleanUtil.matchPhone((String) original.get("linkPhone"))) {
				
				flag=true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("phone") || entry.getKey().equals("linkCall")
							|| entry.getKey().equals("rcall")||entry.getKey().equals("idCard")) {
						continue;
					}
					if (CleanUtil.matchPhone((String) entry.getValue())) {
						String m1 = (String) entry.getValue();
						String phone = (String) original.get("linkPhone");
						original.put("linkPhone", m1);
						entry.setValue(phone);
						flag=true;
						
						tager=false;
						break;
					}
				}
				
			}
			if(tager){
				size++;
			}
		}
		// 判断linkCall
		if (original.containsKey("linkCall")) {
			if (CleanUtil.matchPhone((String) original.get("linkCall"))) {
				
				flag=true;
			} else {
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("linkPhone") || entry.getKey().equals("linkPhone")
							|| entry.getKey().equals("rcall")||entry.getKey().equals("idCard")) {
						continue;
					}
					if (CleanUtil.matchPhone((String) entry.getValue())) {
						String m1 = (String) entry.getValue();
						String phone = (String) original.get("linkCall");
						original.put("linkCall", m1);
						entry.setValue(phone);
						flag=true;
						tager=false;
						break;
					}
				}
				
			}
			
			
			if(tager){size++;}
		}

		

		

		if (original.containsKey("linkName") && original.containsKey("linkAddress")) {
			String linkName = (String) original.get("linkName");
			String linkAddress = (String) original.get("linkAddress");
			if (!"NA".equals(linkName) && !"NA".equals(linkAddress) && linkName.matches(regName)
					&& linkAddress.matches(regAddress)) {
				size++;
			}
		}

		if (original.containsKey("name") && original.containsKey("address")) {
			String name = (String) original.get("name");
			String address = (String) original.get("address");

			if (!"NA".equals(name) && !"NA".equals(address) && name.matches(regName) && address.matches(regAddress)) {
				size++;
			}

		}
		if(original.size()<=size){
			flag=false;
		}

		if (flag) {
			correct.putAll(original);
		} else {
			incorrect.putAll(original);
		}

	}
}
