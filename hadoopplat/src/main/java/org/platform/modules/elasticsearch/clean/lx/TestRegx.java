package org.platform.modules.elasticsearch.clean.lx;

public class TestRegx {
	
	public static void main(String[] args) {
		
		String regx ="(^\\d{15}$)|(^\\d{17}([0-9]|X|x)$)";		
		String test ="511325199212205418";

		if(test.matches(regx)){
			System.out.println("succesful");
		}else {
			System.out.println("error");
		}
		
		/*Pattern pattern = Pattern.compile(regx);
		Matcher matcher = pattern.matcher(test);
		if(matcher.matches()){
			System.out.println("succesful");
		}else {
			System.out.println("error");
		}*/
	
	}

}
