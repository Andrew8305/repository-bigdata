package org.platform.dataplat.utils.bigdata;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

public class HadoopUtils extends AbstrUtils {
	
	public static final String HDFS_DATA_WAREHOUSE = "hdfs://centos.master:9000/home/hadoop/filesystem/data";

	public static final String HDFS_USER_WAREHOUSE = "hdfs://centos.master:9000/user/Administrator";
	
	public static FileSystem getFileSystem() {
		try {
			return FileSystem.get(URI.create(HDFS_DATA_WAREHOUSE), configuration);
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
		}
		return null;
	}
	
	public static FileSystem getFileSystem(String hdfsPath) {
		try {
			return FileSystem.get(URI.create(hdfsPath), configuration);
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
		}
		return null;
	}

}
