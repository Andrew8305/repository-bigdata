package com.taobao.datax.plugins.writer.esbulkwriter;

public class Student extends ESEntity {

	private static final long serialVersionUID = 1L;
	
	private Integer id;
	
	private Long userid;
	
	private String name;
	
	private String phone;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Long getUserid() {
		return userid;
	}

	public void setUserid(Long userid) {
		this.userid = userid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}
	

}
