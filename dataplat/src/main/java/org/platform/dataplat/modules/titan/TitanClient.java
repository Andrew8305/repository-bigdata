package org.platform.dataplat.modules.titan;

import org.platform.dataplat.utils.bigdata.HBaseUtils;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Created by Wulin on 2016/10/26.
 */
public class TitanClient {
	
	public void titanQuery() {
		TitanGraph graph = TitanFactory.build()
                .set("storage.backend", "hbase")
                .set("storage.hostname", "host-115")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "host-115")
                .set("index.search.elasticsearch.interface", "TRANSPORT_CLIENT")
                .set("index.search.elasticsearch.cluster-name", "cisiondata")
                .set("index.search.elasticsearch.client-only", "true")
                .open();
        TitanManagement management = graph.openManagement();
        EdgeLabel following = graph.makeEdgeLabel("following").make();
        EdgeLabel followed = graph.makeEdgeLabel("followed").make();
        System.out.println(following);
        System.out.println(followed);
        management.commit();
	}
	
	public void hbaseQuery() {
		HBaseUtils.printRecords(HBaseUtils.getRecords("titan"));
	}

    public static void main(String[] args) {
        new TitanClient().titanQuery();
    }

}
