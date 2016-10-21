package org.platform.modules.elasticsearch.clean.ly;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSJob;
import org.platform.modules.elasticsearch.base.BaseHDFS2HDFSMapper;

public class Logistics21CleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return Logistics21CleanJobMapper.class;
	}

	public static void main(String[] args) {
		try {
			args = new String[] {
					"hdfs://192.168.0.30:9000/elasticsearch/financial/logistics/21/records-1-m-00000",
					"hdfs://192.168.0.115:9000/ly/output/records-1-m-00001" };
			// args = new
			// String[]{"hdfs://192.168.0.115:9000/ly/input/","hdfs://192.168.0.115:9000/ly/output"};
			int exitCode = ToolRunner.run(new Logistics21CleanJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class Logistics21CleanJobMapper extends BaseHDFS2HDFSMapper {

	@Override
	public void handle(Map<String, Object> original,
			Map<String, Object> correct, Map<String, Object> incorrect) {
		// 不为NA的有效字段个数
		int times = 0;
		// 将所有的字段去空格
		Iterator<Map.Entry<String, Object>> maps = original.entrySet()
				.iterator();
		while (maps.hasNext()) {
			Map.Entry<String, Object> entry = maps.next();
			if (!"NA".equals(entry.getValue())) {
				times++;
			}
			entry.setValue((String.valueOf(entry.getValue())).trim());
		}

		// //身份证（非汉字,非英文，末尾字符可以是Xx）
		// String regIDCard = "^[^(\u4e00-\u9fa5a-zA-Z)[Xx]?]+$";
		// 姓名（存在一个汉字或字母及以上）
		String regName = "^.*([\u4e00-\u9fa5]|[a-zA-Z]){1,}.*$";
		// 至少存在一个汉字
		String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";
		// 电话号码（至少存在6位数字及以上）
		String regPhone = "^([\\s\\S]*)[0-9]{7,11}([\\s\\S]*)$";
		String regIDcard = "^([\\s\\S]*)[0-9]{15,18}+[Xx]?([\\s\\S]*)$";
		// 满足条件的字段个数(不为NA的字段)
		int sum = 0;
		// 收件人判断
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (phone.matches(regPhone)) {
				sum++;
			}
		}
		if (original.containsKey("rcall")) {
			String rcall = (String) original.get("rcall");
			if (rcall.matches(regPhone)) {
				sum++;
			}
		}
		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			if (idCard.matches(regIDcard)) {
				sum++;
			}
		}
		//考虑姓名和地址同时存在的情况
//		if (original.containsKey("name") && original.containsKey("address")) {
//			String name = (String) original.get("name");
//			String address = (String) original.get("address");
//			if (!"NA".equals(name) && !"NA".equals(address)
//					&& name.matches(regName) && address.matches(regAddress)) {
//				sum++;
//			}
//		}

		if (original.containsKey("linkPhone")) {
			String linkPhone = (String) original.get("linkPhone");
			if (linkPhone.matches(regPhone)) {
				sum++;
			}
		}
		if (original.containsKey("linkCall")) {
			String linkCall = (String) original.get("linkCall");
			if (linkCall.matches(regPhone)) {
				sum++;
			}
		}
		if (original.containsKey("linkIdCard")) {
			String linkIdCard = (String) original.get("linkIdCard");
			if (linkIdCard.matches(regIDcard)) {
				sum++;
			}
		}
		//考虑姓名和地址同时存在的情况
//		if (original.containsKey("linkName")
//				&& original.containsKey("linkAddress")) {
//			String linkName = (String) original.get("linkName");
//			String linkAddress = (String) original.get("linkAddress");
//			if (!"NA".equals(linkName) && !"NA".equals(linkAddress)
//					&& linkName.matches(regName)
//					&& linkAddress.matches(regAddress)) {
//				sum++;
//			}
//		}
		if (sum >= 1 && times > 5) {
			correct.putAll(original);
		} else {
			incorrect.putAll(original);
		}
	}
}
