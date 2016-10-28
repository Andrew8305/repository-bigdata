package org.platform.titan.modules.qq;

import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.TitanManagement.IndexBuilder;

/**
 * Created by Wulin on 2016/10/26.
 */
public class GraphOfQQQunFactory {
	
	public static TitanGraph create() {
		TitanFactory.Builder builder = TitanFactory.build();
		builder.set("storage.backend", "hbase");
        builder.set("storage.hostname", "host-115");
        builder.set("storage.tablename", "titan");
        builder.set("index.search.backend", "elasticsearch");
        builder.set("index.search.hostname", "host-115");
        builder.set("index.search.elasticsearch.interface", "TRANSPORT_CLIENT");
        builder.set("index.search.elasticsearch.cluster-name", "cisiondata");
        builder.set("index.search.elasticsearch.client-only", "true");
		TitanGraph graph = builder.open();
		return graph;
	}
	
	public static TitanGraph open(String configFile) {
		return TitanFactory.open(configFile);
	}
	
	public static void createSchema(TitanGraph graph) {
		TitanManagement management = graph.openManagement();
		PropertyKey qqNum = management.containsPropertyKey("qqNum") ? management.getPropertyKey("qqNum") :
				management.makePropertyKey("qqNum").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		TitanGraphIndex qqNumIndex = management.getGraphIndex("qqNumIndex");
		if (null == qqNumIndex) {
			IndexBuilder qqNumIndexBuilder = management.buildIndex("qqNumIndex", Vertex.class).addKey(qqNum);
			qqNumIndexBuilder.unique();
			qqNumIndex = qqNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qqNumIndex, ConsistencyModifier.LOCK);
		}
		
		if (!management.containsPropertyKey("age")) {
			management.makePropertyKey("age").dataType(Integer.class).make();
		}
		if (!management.containsPropertyKey("nickname")) {
			management.makePropertyKey("nickname").dataType(String.class).make();
		}
		if (!management.containsPropertyKey("gender")) {
			management.makePropertyKey("gender").dataType(Integer.class).make();
		}
		
		PropertyKey qunNum = management.containsPropertyKey("qunNum") ? management.getPropertyKey("qunNum") :
				management.makePropertyKey("qunNum").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		TitanGraphIndex qunNumIndex = management.getGraphIndex("qunNumIndex");
		if (null == qunNumIndex) {
			IndexBuilder qunNumIndexBuilder = management.buildIndex("qunNumIndex", Vertex.class).addKey(qunNum);
			qunNumIndexBuilder.unique();
			qunNumIndex = qunNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qunNumIndex, ConsistencyModifier.LOCK);
		}
		
		if (!management.containsPropertyKey("title")) {
			management.makePropertyKey("title").dataType(String.class).make();
		}
		if (!management.containsPropertyKey("text")) {
			management.makePropertyKey("text").dataType(String.class).make();
		}
		if (!management.containsPropertyKey("createDate")) {
			management.makePropertyKey("createDate").dataType(String.class).make();
		}
		
		if (!management.containsVertexLabel("qq")) {
			management.makeVertexLabel("qq").make();
		}
		if (!management.containsVertexLabel("qun")) {
			management.makeVertexLabel("qun").make();
		}
		
		if (!management.containsEdgeLabel("including")) {
			management.makeEdgeLabel("including").multiplicity(Multiplicity.MANY2ONE).make();
		}
		if (!management.containsEdgeLabel("included")) {
			management.makeEdgeLabel("included").multiplicity(Multiplicity.ONE2MANY).make();
		}
		
		management.commit();
	}
	
	public static void load(TitanGraph graph) {
		//Create Schema
		TitanManagement management = graph.openManagement();
		PropertyKey qqNum = management.containsPropertyKey("qqNum") ? management.getPropertyKey("qqNum") :
				management.makePropertyKey("qqNum").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		TitanGraphIndex qqNumIndex = management.getGraphIndex("qqNumIndex");
		if (null == qqNumIndex) {
			IndexBuilder qqNumIndexBuilder = management.buildIndex("qqNumIndex", Vertex.class).addKey(qqNum);
			qqNumIndexBuilder.unique();
			qqNumIndex = qqNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qqNumIndex, ConsistencyModifier.LOCK);
		}
		
		PropertyKey age = management.containsPropertyKey("age") ? management.getPropertyKey("age") :
			management.makePropertyKey("age").dataType(Integer.class).make();
		PropertyKey nickname = management.containsPropertyKey("nickname") ? management.getPropertyKey("nickname") :
			management.makePropertyKey("nickname").dataType(String.class).make();
		PropertyKey gender = management.containsPropertyKey("gender") ? management.getPropertyKey("gender") : 
			management.makePropertyKey("gender").dataType(Integer.class).make();
		
		PropertyKey qunNum = management.containsPropertyKey("qunNum") ? management.getPropertyKey("qunNum") :
				management.makePropertyKey("qunNum").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		TitanGraphIndex qunNumIndex = management.getGraphIndex("qunNumIndex");
		if (null == qunNumIndex) {
			IndexBuilder qunNumIndexBuilder = management.buildIndex("qunNumIndex", Vertex.class).addKey(qunNum);
			qunNumIndexBuilder.unique();
			qunNumIndex = qunNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qunNumIndex, ConsistencyModifier.LOCK);
		}
		
		PropertyKey title = management.containsPropertyKey("title") ? management.getPropertyKey("title") :
			management.makePropertyKey("title").dataType(String.class).make();
		PropertyKey text = management.containsPropertyKey("text") ? management.getPropertyKey("text") :
			management.makePropertyKey("text").dataType(String.class).make();
		PropertyKey createDate = management.containsPropertyKey("createDate") ? management.getPropertyKey("createDate") : 
			management.makePropertyKey("createDate").dataType(String.class).make();
		
		if (!management.containsVertexLabel("qq")) {
			management.makeVertexLabel("qq").make();
		}
		if (!management.containsVertexLabel("qun")) {
			management.makeVertexLabel("qun").make();
		}
		
		if (!management.containsEdgeLabel("including")) {
			management.makeEdgeLabel("including").multiplicity(Multiplicity.MANY2ONE).make();
		}
		if (!management.containsEdgeLabel("included")) {
			management.makeEdgeLabel("included").multiplicity(Multiplicity.ONE2MANY).make();
		}
		
		management.commit();
		
		System.out.println(management.isOpen());
		
		//Create Vertices And Edges
		TitanTransaction transaction = graph.newTransaction();

		System.out.println(transaction.isOpen());
		
		Vertex qq_1 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000001, age.name(), 21, nickname.name(), "zhangsan01", gender.name(), 1);
		Vertex qq_2 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000002, age.name(), 22, nickname.name(), "zhangsan02", gender.name(), 0);
		Vertex qq_3 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000003, age.name(), 23, nickname.name(), "zhangsan03", gender.name(), 0);
		Vertex qq_4 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000004, age.name(), 24, nickname.name(), "zhangsan04", gender.name(), 1);
		Vertex qq_5 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000005, age.name(), 25, nickname.name(), "zhangsan05", gender.name(), 0);
		Vertex qq_6 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000006, age.name(), 26, nickname.name(), "zhangsan06", gender.name(), 1);
		Vertex qq_7 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000007, age.name(), 27, nickname.name(), "zhangsan07", gender.name(), 1);
		Vertex qq_8 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000008, age.name(), 28, nickname.name(), "zhangsan08", gender.name(), 0);
		Vertex qq_9 = transaction.addVertex(T.label, "qq", qqNum.name(), 1000009, age.name(), 29, nickname.name(), "zhangsan09", gender.name(), 1);
		
		Vertex qun_1 = transaction.addVertex(T.label, "qun", qunNum.name(), 101, title.name(), "技术交流01", text.name(), "技术交流", createDate.name(), "2012-01-01");
		Vertex qun_2 = transaction.addVertex(T.label, "qun", qunNum.name(), 102, title.name(), "技术交流02", text.name(), "技术交流", createDate.name(), "2013-01-01");
		Vertex qun_3 = transaction.addVertex(T.label, "qun", qunNum.name(), 103, title.name(), "技术交流03", text.name(), "技术交流", createDate.name(), "2014-01-01");
		
		qq_1.addEdge("included", qun_1);
		qq_2.addEdge("included", qun_1);
		qq_3.addEdge("included", qun_1);
		qq_4.addEdge("included", qun_1);
		qq_5.addEdge("included", qun_2);
		qq_6.addEdge("included", qun_2);
		qq_7.addEdge("included", qun_2);
		qq_8.addEdge("included", qun_3);
		qq_9.addEdge("included", qun_3);
		
		qun_1.addEdge("including", qq_1);
		qun_1.addEdge("including", qq_2);
		qun_1.addEdge("including", qq_3);
		qun_1.addEdge("including", qq_4);
		qun_2.addEdge("including", qq_5);
		qun_2.addEdge("including", qq_6);
		qun_2.addEdge("including", qq_7);
		qun_3.addEdge("including", qq_8);
		qun_3.addEdge("including", qq_9);
		
		transaction.commit();
	}
	
	public static void load_01() {
		BaseConfiguration baseConfiguration = new BaseConfiguration();
        baseConfiguration.setProperty("storage.backend", "hbase");
        baseConfiguration.setProperty("storage.hostname", "host-115");
        baseConfiguration.setProperty("storage.tablename","test");
        TitanGraph graph = TitanFactory.open(baseConfiguration);
        
        Vertex qq_01 = graph.addVertex("qq");
        qq_01.property("qqNum", 1000000001);
        qq_01.property("age", 18);
        qq_01.property("nickname", "zhangsan");
        qq_01.property("gender", 1);
        
        Vertex qq_02 = graph.addVertex("qq");
        qq_02.property("qqNum", 1000000002);
        qq_02.property("age", 20);
        qq_02.property("nickname", "lisi");
        qq_02.property("gender", 0);
        
        Vertex qqqun_01 = graph.addVertex("qqqun");
        qqqun_01.property("qunNum", 100001);
        qqqun_01.property("title", "技术交流01");
        
        qq_01.addEdge("isincluded", qqqun_01);
        qq_02.addEdge("isincluded", qqqun_01);
        
        qqqun_01.addEdge("isincluding", qq_01);
        qqqun_01.addEdge("isincluding", qq_02);
        
        Iterable<TitanVertex> results = graph.query().vertices();
        for (Vertex result : results) {
            System.out.println(result);
        }
        graph.close();
	}
	
	public static void load_02() {
		TitanFactory.Builder builder = TitanFactory.build();
		builder.set("storage.backend", "hbase");
        builder.set("storage.hostname", "host-115");
        builder.set("storage.tablename", "titan");
        builder.set("index.search.backend", "elasticsearch");
        builder.set("index.search.hostname", "host-115");
        builder.set("index.search.elasticsearch.interface", "TRANSPORT_CLIENT");
        builder.set("index.search.elasticsearch.cluster-name", "cisiondata");
        builder.set("index.search.elasticsearch.client-only", "true");
		TitanGraph graph = builder.open();
		
		Vertex qq_01 = graph.addVertex("qq");
        qq_01.property("qqNum", 1000000011);
        qq_01.property("age", 18);
        qq_01.property("nickname", "zhangsan");
        qq_01.property("gender", 1);
        
        Vertex qq_02 = graph.addVertex("qq");
        qq_02.property("qqNum", 1000000012);
        qq_02.property("age", 20);
        qq_02.property("nickname", "lisi");
        qq_02.property("gender", 0);
        
        Vertex qqqun_01 = graph.addVertex("qqqun");
        qqqun_01.property("qunNum", 1000011);
        qqqun_01.property("title", "技术交流01");
        
        qq_01.addEdge("included", qqqun_01);
        qq_02.addEdge("included", qqqun_01);
        
        qqqun_01.addEdge("including", qq_01);
        qqqun_01.addEdge("including", qq_02);
        
        Iterator<TitanVertex> vertices = graph.query().vertices().iterator();
		while (vertices.hasNext()) {
			Vertex vertex = vertices.next();
			System.out.println(vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
				System.out.println(vp.label() + ":" + vp.key() + ":" + vp.value());
			}
		}
        
        graph.close();
	}
	
	public static void query(TitanGraph graph) {
//		GraphTraversalSource g = graph.traversal();
//		System.out.println(g.V().has("qunNum", 101).next());
		Iterator<TitanVertex> vertices = graph.query().vertices().iterator();
		while (vertices.hasNext()) {
			Vertex vertex = vertices.next();
			System.out.println(vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
				System.out.println(vp.label() + ":" + vp.key() + ":" + vp.value());
			}
		}
	}
	
    public static void main(String[] args) {
//    	load_01();
//		load_02();
        TitanGraph graph = GraphOfQQQunFactory.create();
        load(graph);
//        query(graph);
        graph.close();
    }

}
