package org.platform.modules.elasticsearch.clean.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class CleanUtil {

	private static final String phoneRex = "^([\\s\\S]*)[0-9]{7,11}([\\s\\S]*)$";
	private static final String idCardRex = "^([\\s\\S]*)[0-9]{15,18}+[Xx]?([\\s\\S]*)$";
	
	/**
	 * phone正则判断
	 * @param phone
	 * @return
	 */
	public static boolean matchPhone(String phone){
		if (phone == null || "".equals(phone)){
			return false;
		} else {
			return phone.matches(phoneRex);
		}
	}
	
	/**
	 * idCard正则判断
	 * @param idCard
	 * @return
	 */
	public static boolean matchIdCard(String idCard){
		if (idCard == null || "".equals(idCard)){
			return false;
		} else {
			return idCard.matches(idCardRex);
		}
	}
	
	/**
	 * 将一个map的所有value中的空格去除
	 * @param map
	 * @return
	 */
	public static Map<String,Object> replaceSpace(Map<String,Object> map){
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String,Object> entry = it.next();
			entry.setValue((String.valueOf(entry.getValue())).replaceAll(" ", ""));
		}
		return map;
	}
}
