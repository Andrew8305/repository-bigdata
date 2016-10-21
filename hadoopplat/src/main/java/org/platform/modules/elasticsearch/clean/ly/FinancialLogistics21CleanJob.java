package org.platform.modules.elasticsearch.clean.ly;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

public class FinancialLogistics21CleanJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return FinancialLogisticsHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		long  startTime = System.currentTimeMillis();
		String fss = "hdfs://192.168.0.30:9000/elasticsearch/financial/logistics/21";
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			// args = new String[] {
			// "hdfs://192.168.0.30:9000/elasticsearch/financial/logistics/21/records-1-m-00000",
			// "hdfs://192.168.0.30:9000/elasticsearch_clean/financial/logistics/21/records-1-m-00000"
			// };
			// args = new
			// String[]{"hdfs://192.168.0.115:9000/ly/input/records-1-m-00000","hdfs://192.168.0.115:9000/ly/output"};
			int exitCode = 0;
			int times = 0;
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS")
						&& !p.getName().equals("part-r-00000")) {
					System.out.println(p.toString());
					System.out.println(p.getName());
					args = new String[] {
							p.toString(),
							"hdfs://192.168.0.30:9000/elasticsearch_clean/financial/logistics/21/"
									+ p.getName() };
					exitCode = ToolRunner.run(
							new FinancialLogistics21CleanJob(), args);

				}
			}
			long endTime = System.currentTimeMillis();
			System.out.println("用时--------->>>"+((endTime-startTime)/1000)+"s");
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 寻找符合指定字段的值
	public static List<String> findMatch(Map<String, Object> map, String param) {
		Iterator<Entry<String, Object>> newMap = map.entrySet().iterator();
		String phoneRex = "^([\\s\\S]*)[0-9]{7,11}([\\s\\S]*)$";
		String idCardRex = "^([\\s\\S]*)[0-9]{15,18}+[Xx]?([\\s\\S]*)$";
		String regEx = null;
		List<String> list = new ArrayList<String>();
		if ("idCard".equals(param) || "linkIdCard".equals(param)) {
			regEx = idCardRex;
		}
		if ("phone".equals(param) || "rcall".equals(param)
				|| "linkPhone".equals(param) || "linkCall".equals(param)) {
			regEx = phoneRex;
		}

		while (newMap.hasNext()) {
			Entry<String, Object> entry = newMap.next();
			String key = entry.getKey();
			if ("updateTime".equals(key) || "sourceFile".equals(key)
					|| "_id".equals(key) || "insertTime".equals(key)
					|| "expressId".equals(key) || "idCode".equals(key)
					|| "vipId".equals(key) || "orderId".equals(key)
					|| "tbExpressId".equals(key)) {
				continue;
			}
			String value = (String) entry.getValue();
			if ("idCard".equals(param)
					|| ("phone".equals(param) && !"rcall".equals(key)
							&& !"linkPhone".equals(key)
							&& !"linkCall".equals(key) && !"idCard".equals(key))
					|| ("rcall".equals(param) && !"phone".equals(key)
							&& !"linkPhone".equals(key)
							&& !"linkCall".equals(key) && !"idCard".equals(key))
					|| ("linkPhone".equals(param) && !"linkCall".equals(key)
							&& !"phone".equals(key) && !"rcall".equals(key) && !"idCard"
								.equals(key))
					|| ("linkCall".equals(param) && !"linkPhone".equals(key)
							&& !"phone".equals(key) && !"rcall".equals(key) && !"idCard"
								.equals(key))) {

				if (regEx != null && value.matches(regEx)) {
					list.add(key);
					list.add(value);
					System.out.println(list.toString());
					break;
				}
			}

		}
		return list;
	}

}

class FinancialLogisticsHDFS2HDFSMapper extends BaseHDFS2HDFSMapper {

	@Override
	public void handle(Map<String, Object> original,
			Map<String, Object> correct, Map<String, Object> incorrect) {
		// 有效字段的个数
		int times = 0;
		Iterator<Map.Entry<String, Object>> maps = original.entrySet()
				.iterator();
		while (maps.hasNext()) {
			Map.Entry<String, Object> entry = maps.next();
			if (!"NA".equals(entry.getValue())) {
				times++;
			}
		}
		// 替换空格
		Map<String, Object> map = CleanUtil.replaceSpace(original);
		// 满足正则表达式的次数
		int sum = 0;
		if (original.containsKey("idCard")) {
			String idCard = (String) map.get("idCard");
			if (CleanUtil.matchIdCard(idCard)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"idCard");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("idCard", list.get(1));
					map.put(list.get(0), idCard);
					sum++;
				}
			}
		}
		if (original.containsKey("phone")) {
			String phone = (String) map.get("phone");
			if (CleanUtil.matchPhone(phone)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"phone");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("phone", list.get(1));
					map.put(list.get(0), phone);
					sum++;
				}
			}
		}
		if (original.containsKey("rcall")) {
			String rcall = (String) map.get("rcall");
			if (CleanUtil.matchPhone(rcall)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"rcall");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("rcall", list.get(1));
					map.put(list.get(0), rcall);
					sum++;
				}
			}
		}
		if (original.containsKey("linkPhone")) {
			String linkPhone = (String) map.get("linkPhone");
			if (CleanUtil.matchPhone(linkPhone)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"linkPhone");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("linkPhone", list.get(1));
					map.put(list.get(0), linkPhone);
					sum++;
				}
			}
		}
		if (original.containsKey("linkCall")) {
			String linkCall = (String) map.get("linkCall");
			if (CleanUtil.matchPhone(linkCall)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"linkCall");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("linkCall", list.get(1));
					map.put(list.get(0), linkCall);
					sum++;
				}
			}
		}
		if (original.containsKey("linkIdCard")) {
			String linkIdCard = (String) map.get("linkIdCard");
			if (CleanUtil.matchIdCard(linkIdCard)) {
				sum++;
			} else {
				List<String> list = FinancialLogistics21CleanJob.findMatch(map,
						"linkIdCard");
				if (list.size() > 0) {
					// 覆盖原来idCard的值
					map.put("linkIdCard", list.get(1));
					map.put(list.get(0), linkIdCard);
					sum++;
				}
			}
		}

		if (sum > 0 && times > 5) {
			correct.putAll(map);
		} else {
			incorrect.putAll(map);
		}
	}

}
