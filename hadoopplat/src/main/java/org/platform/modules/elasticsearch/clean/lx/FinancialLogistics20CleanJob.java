package org.platform.modules.elasticsearch.clean.lx;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSMapper;

public class FinancialLogistics20CleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return TelecomHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args) {
		String fss ="hdfs://192.168.0.30:9000/elasticsearch/financial/logistics/20";
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
							"hdfs://192.168.0.30:9000/elasticsearch_clean/financial/logistics/30/"+p.getName()};
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
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
		Map<String, Object> incorrect) {

		int times = 0;
		Iterator<Map.Entry<String, Object>> maps = original.entrySet().iterator();
		//去空格		
		while (maps.hasNext()) {
			Map.Entry<String, Object> entry = maps.next();		
			if(!"NA".equals(entry.getValue())){
				times++;
			}
			entry.setValue((String.valueOf(entry.getValue())).trim());
		}				
		//至少包含6个数字
		String numRegx =".*\\d{6,}+.*";
		String stringRegx="^.*[\u4e00-\u9fa5a-zA-Z]{1,}.*$";
		
		//至少存在一个汉字
		String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";
		
		//姓名（存在一个汉字或字母及以上）
				String regName = "^.*([\u4e00-\u9fa5]|[a-zA-Z]){1,}.*$";
				//至少存在一个汉字

		int sum =0;
		
		//收件人判断
		if(original.containsKey("phone")){			
			String phone = (String) original.get("phone");
			if(phone.matches(numRegx)){
				sum++;
			}
		}	
		if(original.containsKey("rcall")){
			String rcall = (String) original.get("rcall");
			if(rcall.matches(numRegx)){
				sum++;
			}
		}
		if(original.containsKey("idCard")){
			String idCard = (String) original.get("idCard");
			if(idCard.matches(numRegx)){
				sum++;
			}
		}
		
		if(original.containsKey("name") && original.containsKey("address")){
			String name = (String) original.get("name");
			String address = (String) original.get("address");
			if(!"NA".equals(name) && !"NA".equals(name) &&name.matches(stringRegx) && address.matches(regAddress)){
				sum++;
			}
		}
				
		//寄件人信息判断
		if(original.containsKey("linkPhone")){
			String linkPhone = (String) original.get("linkPhone");
			if(linkPhone.matches(numRegx)){
				sum++;
			}
		}
		if(original.containsKey("linkCall")){
			String linkCall = (String) original.get("linkCall");
			if(linkCall.matches(numRegx)){
				sum++;
			}
		}
		if(original.containsKey("linkIdCard")){
			String linkIdCard = (String) original.get("linkIdCard");
			if(linkIdCard.matches(numRegx)){
				sum++;
			}
		}	
		if(original.containsKey("linkName") && original.containsKey("linkAddress")){
			String linkName = (String) original.get("linkName");
			String linkAddress = (String) original.get("linkAddress");
			if(!"NA".equals(linkName) && !"NA".equals(linkAddress) &&linkName.matches(stringRegx) && linkAddress.matches(regAddress)){
				sum++;
			}
		}	
		if(sum>=1 && times>5){
			correct.putAll(original);
		}else{
			incorrect.putAll(original);
		}
	}
		
}
