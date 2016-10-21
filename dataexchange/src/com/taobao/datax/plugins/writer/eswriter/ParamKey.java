package com.taobao.datax.plugins.writer.eswriter;

public final class ParamKey {
	
	/*
	 * @name: singleCurl 
	 * @description:  single curl
	 * @range: 
	 * @mandatory: false
	 * @default:
	 */
	public final static String singleCurl = "single_curl";
	
	/*
	 * @name: nullChar
	 * @description:  replace null with the nullchar
	 * @range: 
	 * @mandatory: false
	 * @default: 
	 */
	public final static String nullChar = "null_char";
	
	/*
	 * @name: columnNames
	 * @description:  column name list 
	 * @range: 
	 * @mandatory: false
	 * @default: 
	 */
	public final static String columnNames = "column_names";
	
	/*
	 * @name: columnNameSplit
	 * @description: separator to split column names
	 * @range:
	 * @mandatory: false
	 * @default:\t
	 */
	public final static String columnNameSplit = "column_name_split";

	 /*
       * @name:concurrency
       * @description:concurrency of the job
       * @range:1
       * @mandatory: false
       * @default:1
       */
	public final static String concurrency = "concurrency";
	
}

