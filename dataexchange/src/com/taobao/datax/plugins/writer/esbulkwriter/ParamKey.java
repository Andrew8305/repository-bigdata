package com.taobao.datax.plugins.writer.esbulkwriter;

public final class ParamKey {
	
	/*
	 * @name: esIndex 
	 * @description:  elastic search index
	 * @range: 
	 * @mandatory: false
	 * @default:
	 */
	public final static String esIndex = "es_index";
	
	/*
	 * @name: esType
	 * @description:  elastic search type
	 * @range: 
	 * @mandatory: false
	 * @default:
	 */
	public final static String esType = "es_type";
	
	/*
	 * @name: attrNameString
	 * @description:  attr name list 
	 * @range: 
	 * @mandatory: false
	 * @default: 
	 */
	public final static String attrNameString = "attr_name_string";
	
	/*
	 * @name: attrNameSplit
	 * @description: separator to split attr name string
	 * @range:
	 * @mandatory: false
	 * @default:\t
	 */
	public final static String attrNameSplit = "attr_name_split";
	
	/*
	 * @name: className
	 * @description: qualified class name 
	 * @range:
	 * @mandatory: false
	 * @default:\t
	 */
	public final static String className = "class_name";
	
	/*
     * @name:concurrency
     * @description:concurrency of the job
     * @range:1
     * @mandatory: false
     * @default:1
     */
	public final static String concurrency = "concurrency";
	
}

