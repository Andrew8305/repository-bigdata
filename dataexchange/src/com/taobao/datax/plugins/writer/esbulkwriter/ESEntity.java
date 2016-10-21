package com.taobao.datax.plugins.writer.esbulkwriter;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

public class ESEntity implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String _id;
	
	public ESEntity() {
		set_id(ESIDGenerator.UUID());
	}

	public String get_id() {
		return StringUtils.isBlank(_id) ? ESIDGenerator.UUID() : _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}
	
	

}
