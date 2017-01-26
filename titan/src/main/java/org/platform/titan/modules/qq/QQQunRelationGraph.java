package org.platform.titan.modules.qq;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.platform.titan.modules.GraphUtils;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery.Result;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.TitanManagement.IndexBuilder;

/**
 * Created by Wulin on 2016/10/26.
 */
public class QQQunRelationGraph {
	
	public static TitanGraph buildTitanGraph(String configFile) {
		return TitanFactory.open(configFile);
	}
	
	public static void buildSchema(TitanGraph graph) {
		TitanManagement management = graph.openManagement();
		
		PropertyKey qqNum = management.containsPropertyKey("qqNum") ? management.getPropertyKey("qqNum") :
				management.makePropertyKey("qqNum").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		System.out.println("contains qqNum index: " + management.containsGraphIndex("qqNum"));
		if (!management.containsGraphIndex("qqNum")) {
			IndexBuilder qqNumIndexBuilder = management.buildIndex("qqNum", Vertex.class).addKey(qqNum).unique();
			TitanGraphIndex qqNumIndex = qqNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qqNumIndex, ConsistencyModifier.LOCK);
		} else {
			TitanGraphIndex qqNumIndex = management.getGraphIndex("qqNum");
			System.out.println("unique: " + qqNumIndex.isUnique());
			System.out.println("mixed index: " + qqNumIndex.isMixedIndex());
			System.out.println("composite index: " + qqNumIndex.isCompositeIndex());
			System.out.println("backing index: " + qqNumIndex.getBackingIndex());
			System.out.println("status: " + qqNumIndex.getIndexStatus(management.getPropertyKey("qqNum")));
			PropertyKey[] propertyKeys = qqNumIndex.getFieldKeys();
			for (PropertyKey propertyKey : propertyKeys) {
				System.out.println(propertyKey.label() + ":" + propertyKey.dataType());
			}
//			management.updateIndex(index, SchemaAction.ENABLE_INDEX);
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
		if (!management.containsGraphIndex("qunNum")) {
			IndexBuilder qunNumIndexBuilder = management.buildIndex("qunNum", Vertex.class).addKey(qunNum).unique();
			TitanGraphIndex qunNumIndex = qunNumIndexBuilder.buildCompositeIndex();
			management.setConsistency(qunNumIndex, ConsistencyModifier.LOCK);
		} else {
			TitanGraphIndex qunNumIndex = management.getGraphIndex("qunNum");
			System.out.println("unique: " + qunNumIndex.isUnique());
			System.out.println("mixed index: " + qunNumIndex.isMixedIndex());
			System.out.println("composite index: " + qunNumIndex.isCompositeIndex());
			System.out.println("backing index: " + qunNumIndex.getBackingIndex());
			System.out.println("status: " + qunNumIndex.getIndexStatus(management.getPropertyKey("qunNum")));
			PropertyKey[] propertyKeys = qunNumIndex.getFieldKeys();
			for (PropertyKey propertyKey : propertyKeys) {
				System.out.println(propertyKey.label() + ":" + propertyKey.dataType());
			}
//			management.updateIndex(qunNumIndex, SchemaAction.ENABLE_INDEX);
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
		
		if (!management.containsEdgeLabel("containing")) {
			management.makeEdgeLabel("containing").make();
		}
		if (!management.containsEdgeLabel("contained")) {
			management.makeEdgeLabel("contained").make();
		}
		
		management.commit();
	}
	
	public static void loadData(TitanGraph graph) {
		TitanTransaction transaction = graph.newTransaction();
		
		Vertex qq_1 = transaction.addVertex(T.label, "qq", "qqNum", 10000111, "age", 21, "nickname", "zhangsan01", "gender", 1);
		Vertex qq_2 = transaction.addVertex(T.label, "qq", "qqNum", 10000112, "age", 22, "nickname", "zhangsan02", "gender", 0);
		Vertex qq_3 = transaction.addVertex(T.label, "qq", "qqNum", 10000113, "age", 23, "nickname", "zhangsan03", "gender", 0);
		Vertex qq_4 = transaction.addVertex(T.label, "qq", "qqNum", 10000114, "age", 24, "nickname", "zhangsan04", "gender", 1);
		Vertex qq_5 = transaction.addVertex(T.label, "qq", "qqNum", 10000115, "age", 25, "nickname", "zhangsan05", "gender", 0);
		Vertex qq_6 = transaction.addVertex(T.label, "qq", "qqNum", 10000116, "age", 26, "nickname", "zhangsan06", "gender", 1);
		Vertex qq_7 = transaction.addVertex(T.label, "qq", "qqNum", 10000117, "age", 27, "nickname", "zhangsan07", "gender", 1);
		Vertex qq_8 = transaction.addVertex(T.label, "qq", "qqNum", 10000118, "age", 28, "nickname", "zhangsan08", "gender", 0);
		Vertex qq_9 = transaction.addVertex(T.label, "qq", "qqNum", 10000119, "age", 29, "nickname", "zhangsan09", "gender", 1);
		
		Vertex qun_1 = transaction.addVertex(T.label, "qun", "qunNum", 1111, "title", "技术交流11", "text", "技术交流", "createDate", "2012-01-01");
		Vertex qun_2 = transaction.addVertex(T.label, "qun", "qunNum", 1112, "title", "技术交流12", "text", "技术交流", "createDate", "2013-01-01");
		Vertex qun_3 = transaction.addVertex(T.label, "qun", "qunNum", 1113, "title", "技术交流13", "text", "技术交流", "createDate", "2014-01-01");
		
		qq_1.addEdge("contained", qun_1);
		qq_2.addEdge("contained", qun_1);
		qq_3.addEdge("contained", qun_1);
		qq_4.addEdge("contained", qun_1);
		qq_5.addEdge("contained", qun_2);
		qq_6.addEdge("contained", qun_2);
		qq_7.addEdge("contained", qun_2);
		qq_8.addEdge("contained", qun_3);
		qq_9.addEdge("contained", qun_3);
		
		qun_1.addEdge("containing", qq_1);
		qun_1.addEdge("containing", qq_2);
		qun_1.addEdge("containing", qq_3);
		qun_1.addEdge("containing", qq_4);
		qun_2.addEdge("containing", qq_5);
		qun_2.addEdge("containing", qq_6);
		qun_2.addEdge("containing", qq_7);
		qun_3.addEdge("containing", qq_8);
		qun_3.addEdge("containing", qq_9);
		
		transaction.commit();
	}
	
	public static void loadData_01() {
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
	
	public static void loadData_02() {
		TitanGraph graph = GraphUtils.getInstance().getGraph();
		
		Vertex qq_01 = graph.addVertex("qq");
        qq_01.property("qqNum", 1000000111);
        qq_01.property("age", 18);
        qq_01.property("nickname", "zhangsan");
        qq_01.property("gender", 1);
        
        Vertex qq_02 = graph.addVertex("qq");
        qq_02.property("qqNum", 1000000112);
        qq_02.property("age", 20);
        qq_02.property("nickname", "lisi");
        qq_02.property("gender", 0);
        
        Vertex qqqun_01 = graph.addVertex("qqqun");
        qqqun_01.property("qunNum", 1000111);
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
	
	public static void queryData(TitanGraph graph) {
//		GraphTraversalSource g = graph.traversal();
//		System.out.println(g.V().has("qunNum", 101).next());
		Iterator<TitanVertex> vertices = graph.query().vertices().iterator();
		while (vertices.hasNext()) {
			Vertex vertex = vertices.next();
			System.out.println(vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
//				System.out.println(vp.label() + ":" + vp.key() + ":" + vp.value());
				System.out.println(vp.key() + ":" + vp.value());
			}
			System.out.println("######");
		}
		Iterator<TitanEdge> edges = graph.query().edges().iterator();
		while (edges.hasNext()) {
			Edge edge = edges.next();
			System.out.println("edge: " + edge.label());
			Iterator<Vertex> vs = edge.vertices(Direction.BOTH);
			while (vs.hasNext()) {
				System.out.println("vs label: " + vs.next().label());
			}
			Iterator<Property<Object>> edgeProperties = edge.properties();
			while (edgeProperties.hasNext()) {
				Property<Object> ep = edgeProperties.next();
				System.out.println(ep.key() + ":" + ep.value());
			}
			System.out.println("######");
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void queryData_01(TitanGraph graph) {
		System.out.println("$$$$$$");
		System.out.println(graph.query().has("qqNum", 10000011).vertices().iterator());
//		Iterator<TitanVertex> iterator = graph.query().has("nickname", "zhangsan01").vertices().iterator();
		Iterator<TitanVertex> iterator = graph.query().has("age", 21).vertices().iterator();
		while (iterator.hasNext()) {
			TitanVertex vertex = iterator.next();
			System.out.println("===" + vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
//				System.out.println(vp.label() + ":" + vp.key() + ":" + vp.value());
				System.out.println(vp.key() + ":" + vp.value());
			}
		}
	}
	
	public static void queryData_02(TitanGraph graph) {
		System.out.println("$$$$$$");
		GraphTraversalSource g = graph.traversal();
		List<Vertex> verteies = g.V().has("age", 22).iterate().toList();
		for (Vertex vertex : verteies) {
			System.out.println(vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
				System.out.println(vp.key() + ":" + vp.value());
			}
			System.out.println("$$$$$$");
		}
		GraphTraversal<Vertex, Vertex> gt = g.V().has("qqNum", 10000011);
		while (gt.hasNext()) {
			Vertex vertex = gt.next();
			System.out.println(vertex.label());
			Iterator<VertexProperty<Object>> vertexProperties = vertex.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> vp = vertexProperties.next();
				System.out.println(vp.key() + ":" + vp.value());
			}
//			System.out.println(vertex.value("qqNum")); 
//			System.out.println(vertex.values("qqNum", "age", "nickname")); 
			System.out.println("$$$$$$");
		}
	}
	
	public static void queryData_03(TitanGraph graph) {
		TitanIndexQuery indexQuery = graph.indexQuery("personIndex", "10000111");
		Iterator<TitanIndexQuery.Result<TitanVertex>> iterator = indexQuery.vertices().iterator();
		while (iterator.hasNext()) {
			Result<TitanVertex> vertex = iterator.next();
			System.out.println("vertex label: " + vertex.getElement().label());
		}
	}
	
    public static void main(String[] args) {
        TitanGraph graph = GraphUtils.getInstance().getGraph();
        buildSchema(graph);
//        loadData(graph);
//        queryData(graph);
//        queryData_01(graph);
//        queryData_02(graph);
        queryData_03(graph);
        graph.close();
    }
    
}
