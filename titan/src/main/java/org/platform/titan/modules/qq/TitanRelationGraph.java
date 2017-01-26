package org.platform.titan.modules.qq;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.platform.titan.modules.GraphUtils;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.TitanManagement.IndexBuilder;

public class TitanRelationGraph {

	public static void buildSchema(TitanGraph graph) {
		TitanManagement management = graph.openManagement();
		
//		g.makeKey("type").dataType(String.class).indexed(Vertex.class).indexed("search",Vertex.class).make()
		
		PropertyKey people = management.containsPropertyKey("people") ? management.getPropertyKey("people") :
				management.makePropertyKey("people").dataType(String.class).make();
		if (!management.containsGraphIndex("peopleIndex")) {
			IndexBuilder peopleIndexBuilder = management.buildIndex("peopleIndex", Vertex.class)
					.addKey(people, Parameter.of("people", Mapping.STRING)).unique();
			TitanGraphIndex qqNumIndex = peopleIndexBuilder.buildCompositeIndex();
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
		}
		
		if (!management.containsPropertyKey("name")) {
			management.makePropertyKey("name").dataType(String.class).make();
		}
		
		if (!management.containsEdgeLabel("send")) {
			management.makeEdgeLabel("send").make();
		}
		if (!management.containsEdgeLabel("receive")) {
			management.makeEdgeLabel("receive").make();
		}
		
		management.commit();
	}
	
	public PropertyKey makePropertyKey(TitanManagement mgmt, String propertyKeyName, Class<?> propertyType) {
	    PropertyKey propertyKey = mgmt.getPropertyKey(propertyKeyName);
	    if (propertyKey == null) {
	        propertyKey = mgmt.makePropertyKey(propertyKeyName).dataType(String.class).make();
	    }
	    return propertyKey;
	}
	
	public void createMixedIndexForVertexProperty(TitanGraph graph, String indexName, String propertyKeyName, Class<?> propertyType) {
		TitanManagement mgmt = graph.openManagement();
		try {
	        PropertyKey propertyKey = makePropertyKey(mgmt, propertyKeyName, propertyType);
	        TitanGraphIndex graphIndex = mgmt.getGraphIndex(indexName);
	        if (graphIndex == null) {
	            graphIndex = mgmt.buildIndex(indexName, Vertex.class)
	                    .addKey(propertyKey, Parameter.of("mapping", Mapping.STRING)).buildMixedIndex("search");
	        } else {
	            mgmt.addIndexKey(graphIndex, propertyKey);
	        }
	        mgmt.commit();
	    } catch (Exception e) {
	        mgmt.rollback();
	    } finally {
	    	
	    }
	}
	
	public void loadData_01(TitanGraph graph) throws Exception {
		createMixedIndexForVertexProperty(graph, "personnode", "emailId", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "firstName", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "lastName", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "address", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "hometown", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "city", String.class);
	    createMixedIndexForVertexProperty(graph, "personnode", "spousename", String.class);
	}
	
	public void createNode(TitanGraph graph, String emailid) {
	    Vertex vertex = graph.addVertex("person");
	    vertex.property("emailId", emailid);
	    vertex.property("firstName", "First Name");
	    vertex.property("lastName", "Last Name");
	    vertex.property("address", "Address");
	    vertex.property("hometown", "Noida");
	    vertex.property("city", "Noida");
	    vertex.property("spousename", "Preeti");
	    graph.tx().commit();
	}
	
	public Object getNode(TitanGraph graph, String emailId) {
	    String reString = null;
	    try {
	    	Vertex vertex = graph.traversal().V().has("emailId", emailId).next();
	        reString = vertex.value("emailId");
	    } catch (NoSuchElementException e) {
	        e.printStackTrace();
	    } finally {
	        graph.tx().close();
	    }
	    return reString;
	}
	
	public static void loadData(TitanGraph graph) {
		TitanTransaction transaction = graph.newTransaction();
		
		Vertex people_1 = transaction.addVertex(T.label, "people", "name", "zhangsan01");
		Vertex people_2 = transaction.addVertex(T.label, "people", "name", "zhangsan02");
		Vertex people_3 = transaction.addVertex(T.label, "people", "name", "zhangsan03");
		Vertex people_4 = transaction.addVertex(T.label, "people", "name", "zhangsan04");
		
		people_1.addEdge("send", people_2);
		people_1.addEdge("send", people_3);
		people_1.addEdge("send", people_4);
		
		
		people_2.addEdge("receive", people_1);
		
		transaction.commit();
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
	
	public static void main(String[] args) throws Exception {
		TitanGraph graph = GraphUtils.getInstance().getGraph();
//		buildSchema(graph);
//		loadData(graph);
//		queryData(graph);
//		graph.close();
		TitanRelationGraph test = new TitanRelationGraph();
		test.loadData_01(graph);
		Thread.sleep(10000);

	    String emailId = "emailId" + System.nanoTime();
	    test.createNode(graph, emailId);
	    System.out.println("Create " + emailId);
	    System.out.println("First time " + test.getNode(graph, emailId));
	    Thread.sleep(2000);
	    System.out.println("After a delay of 2 sec " + test.getNode(graph, emailId));
	}
	
}
