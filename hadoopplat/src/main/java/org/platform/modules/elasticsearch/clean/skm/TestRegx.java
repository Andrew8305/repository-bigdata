package org.platform.modules.elasticsearch.clean.skm;



public class TestRegx {
	
	public static void main(String[] args) {
		
	
		String test ="康桥东路1288";
	    String phoneRex = "^([\\s\\S]*)[0-9]{7,11}([\\s\\S]*)$";
		String reg="^[\\s\\d-]+$";  
		String idCardReg="^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$"
				+ "|^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([0-9]|Xx)$";
		
		String emailReg="^([a-zA-Z0-9_-])+@([a-zA-Z0-9_-])+((\\.[a-zA-Z0-9_-]{2,3}){1,2})$";
		// System.out.println(test.matches(idCardReg));
		if(test.matches(phoneRex)){
			System.out.println("succesful");
		}else {
			System.out.println("error");
		}
		
	}

}

