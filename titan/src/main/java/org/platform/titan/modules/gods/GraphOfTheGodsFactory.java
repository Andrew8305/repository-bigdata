package org.platform.titan.modules.gods;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class GraphOfTheGodsFactory {

	public static final String INDEX_NAME = "search";

	public static TitanGraph create(final String directory) {
		TitanFactory.Builder config = TitanFactory.build();
		config.set("storage.backend", "berkeleyje");
		config.set("storage.directory", directory);
		config.set("index." + INDEX_NAME + ".backend", "elasticsearch");
		config.set("index." + INDEX_NAME + ".directory", directory + File.separator + "es");
		config.set("index." + INDEX_NAME + ".elasticsearch.local-mode", true);
		config.set("index." + INDEX_NAME + ".elasticsearch.client-only", false);
		TitanGraph graph = config.open();
		GraphOfTheGodsFactory.load(graph);
		return graph;
	}
	
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

	public static void loadWithoutMixedIndex(final TitanGraph graph,
			boolean uniqueNameCompositeIndex) {
		load(graph, null, uniqueNameCompositeIndex);
	}

	public static void load(final TitanGraph graph) {
		load(graph, INDEX_NAME, true);
	}

	public static void load(final TitanGraph graph, String mixedIndexName,
			boolean uniqueNameCompositeIndex) {
		// Create Schema
		TitanManagement mgmt = graph.openManagement();
		final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
		TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
		if (uniqueNameCompositeIndex) nameIndexBuilder.unique();
		TitanGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
		mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
		final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
		if (null != mixedIndexName)
			mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);
		final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
		final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
		final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
		if (null != mixedIndexName)
			mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place)
					.buildMixedIndex(mixedIndexName);
		mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
		mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
		EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
		mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH,
				Order.decr, time);
		mgmt.makeEdgeLabel("lives").signature(reason).make();
		mgmt.makeEdgeLabel("pet").make();
		mgmt.makeEdgeLabel("brother").make();
		mgmt.makeVertexLabel("titan").make();
		mgmt.makeVertexLabel("location").make();
		mgmt.makeVertexLabel("god").make();
		mgmt.makeVertexLabel("demigod").make();
		mgmt.makeVertexLabel("human").make();
		mgmt.makeVertexLabel("monster").make();
		mgmt.commit();

		TitanTransaction tx = graph.newTransaction();
		// vertices
		Vertex saturn = tx.addVertex(T.label, "titan", "name", "saturn", "age", 10000);
		Vertex sky = tx.addVertex(T.label, "location", "name", "sky");
		Vertex sea = tx.addVertex(T.label, "location", "name", "sea");
		Vertex jupiter = tx.addVertex(T.label, "god", "name", "jupiter", "age", 5000);
		Vertex neptune = tx.addVertex(T.label, "god", "name", "neptune", "age", 4500);
		Vertex hercules = tx.addVertex(T.label, "demigod", "name", "hercules", "age", 30);
		Vertex alcmene = tx.addVertex(T.label, "human", "name", "alcmene", "age", 45);
		Vertex pluto = tx.addVertex(T.label, "god", "name", "pluto", "age", 4000);
		Vertex nemean = tx.addVertex(T.label, "monster", "name", "nemean");
		Vertex hydra = tx.addVertex(T.label, "monster", "name", "hydra");
		Vertex cerberus = tx.addVertex(T.label, "monster", "name", "cerberus");
		Vertex tartarus = tx.addVertex(T.label, "location", "name", "tartarus");

		// edges
		jupiter.addEdge("father", saturn);
		jupiter.addEdge("lives", sky, "reason", "loves fresh breezes");
		jupiter.addEdge("brother", neptune);
		jupiter.addEdge("brother", pluto);
		neptune.addEdge("lives", sea).property("reason", "loves waves");
		neptune.addEdge("brother", jupiter);
		neptune.addEdge("brother", pluto);
		hercules.addEdge("father", jupiter);
		hercules.addEdge("mother", alcmene);
		hercules.addEdge("battled", nemean, "time", 1, "place", Geoshape.point(38.1f, 23.7f));
		hercules.addEdge("battled", hydra, "time", 2, "place", Geoshape.point(37.7f, 23.9f));
		hercules.addEdge("battled", cerberus, "time", 12, "place", Geoshape.point(39f, 22f));
		pluto.addEdge("brother", jupiter);
		pluto.addEdge("brother", neptune);
		pluto.addEdge("lives", tartarus, "reason", "no fear of death");
		pluto.addEdge("pet", cerberus);
		cerberus.addEdge("lives", tartarus);
		// commit the transaction to disk

		tx.commit();
	}
	
	public static void query(TitanGraph graph) {
		GraphTraversalSource g = graph.traversal();  
		System.out.println(g.V().has("name", "hercules").next().value("name"));  
		System.out.println(g.V().has("name", "hercules").next().values("name", "age"));  
		Iterator<Object> iterator = g.V().has("name","hercules").next().values("name", "age");  
	    while(iterator.hasNext()){  
	    	Object object = iterator.next();  
	        System.out.println(object);  
	    }  
	    Vertex saturn = g.V().has("name","saturn").next();  
	    System.out.println(saturn);  
        //得到 saturn的孙子节点  
        System.out.println(g.V(saturn).in("father").in("father").next().value("age"));  
  
        GraphTraversal<Edge, Edge> a =  g.E().has("place", P.eq(Geoshape.point(38.1f, 23.7f)));  
        System.out.println(a);  
        while(a.hasNext()){  
            Edge e = a.next();  
            System.out.println(e.keys());  
            System.out.println(e.label());  
            System.out.println(e.outVertex().value("name"));  
            System.out.println(e.inVertex().value("name"));  
            System.out.println(e.value("time") + "  :  " + e.value("place"));  
        }  
        
        Vertex hercules = g.V().has("name","hercules").next();  
        System.out.println(g.V(hercules).out("mother","father").values("name"));  
        GraphTraversal<Vertex,Vertex> mF = g.V(hercules).out("mother", "father");  
        while(mF.hasNext()){  
            Vertex v = mF.next();  
            System.out.println(v.label() + "  :  " + v.value("name"));  
        }  
  
        System.out.println(g.V(saturn).repeat(__.in("father")).times(2).next().value("name"));  
  
        GraphTraversal<Vertex,Vertex> monsters = g.V(hercules).out("battled");  
        while(monsters.hasNext()){  
            Vertex monster = monsters.next();  
            System.out.println(monster.label() + "  :  " + monster.value("name"));  
        }  
  
        monsters = g.V(hercules).outE("battled").has("time",P.eq(1)).inV();  
        while(monsters.hasNext()){  
            Vertex monster = monsters.next();  
            System.out.println(monster.label() + "  :  " + monster.value("name"));  
        }  
        Vertex pluto = g.V().has("name","pluto").next();  
        
        GraphTraversal<Vertex,Vertex> liveInTartarusVertex = g.V(pluto).out("lives").in("lives");  
        while(liveInTartarusVertex.hasNext()){  
            Vertex vertex = liveInTartarusVertex.next();  
            System.out.println(vertex.value("name"));  
        }  
  
        GraphTraversal<Vertex,Vertex> liveInTartarusVertexNo = g.V(pluto).out("lives").in("lives").where(  
                __.is(P.neq(pluto)));  
        while(liveInTartarusVertexNo.hasNext()){  
            Vertex vertex = liveInTartarusVertexNo.next();  
            System.out.println(vertex.value("name"));  
        }  
  
  
        GraphTraversal<Vertex,Vertex> liveInTartarusVertexNot = g.V(pluto).as("x").out("lives").in("lives").where(P.neq("x"));  
        while(liveInTartarusVertexNot.hasNext()){  
            Vertex vertex = liveInTartarusVertexNot.next();  
            System.out.println("======"+vertex.value("name"));  
        }  
  
        GraphTraversal<Vertex, Map<String, Vertex>> brothers = 
        		g.V(pluto).out("brother").as("god").out("lives").as("place").select("god","place");  
        while(brothers.hasNext()){  
            Map<String,Vertex> map = brothers.next();  
            System.out.println(map);  
            for(Map.Entry<String,Vertex> entry:map.entrySet()){  
                System.out.println(entry.getKey()+" : "+entry.getValue().value("name"));  
            }  
        }  
  
        System.out.println(g.V(pluto).outE("lives").next().value("reason"));  
  
        GraphTraversal<Edge,Edge> reasons = g.E().has("reason").as("r").values("reason").is(Text.textContains("loves")).select("r");  
        System.out.println(reasons);  
        while(reasons.hasNext()){  
            Edge e = reasons.next();  
            System.out.println(e.keys());  
            System.out.println(e.label());  
            System.out.println(e.value("reason"));  
        }  
  
        GraphTraversal<Edge,Map<String,Object>> reasons2 = 
        		g.E().has("reason").as("source").values("reason").is(Text.textContains("loves")).as("reason").select("source")  
                	.outV().values("name").as("god").select("source").inV().values("name").as("thing").select("god","reason","thing");  
  
        while(reasons2.hasNext()){  
            Map<String,Object> map = reasons2.next();  
            System.out.println(map);  
            for(Map.Entry<String,Object> entry:map.entrySet()){  
                System.out.println(entry.getKey() + " :  " + entry.getValue());  
            }  
        }  
	}

	/**
	 * 
	 * Calls {@link TitanFactory#open(String)}, passing the Titan configuration
	 * file path
	 * which must be the sole element in the {@code args} array, then calls
	 * {@link #load(com.thinkaurelius.titan.core.TitanGraph)} on the opened
	 * graph,
	 * then calls {@link com.thinkaurelius.titan.core.TitanGraph#close()}
	 * and returns.
	 * <p/>
	 * This method may call {@link System#exit(int)} if it encounters an error,
	 * such as
	 * failure to parse its arguments. Only use this method when executing main
	 * from
	 * a command line. Use one of the other methods on this class (
	 * {@link #create(String)}
	 * or {@link #load(com.thinkaurelius.titan.core.TitanGraph)}) when calling
	 * from
	 * an enclosing application.
	 * @param args
	 *            a singleton array containing a path to a Titan config
	 *            properties file
	 */
	public static void main(String args[]) {
//		if (null == args || 1 != args.length) {
//			System.err.println("Usage: GraphOfTheGodsFactory <titan-config-file>");
//			System.exit(1);
//		}
//		TitanGraph g = TitanFactory.open(args[0]);
//		load(g);
//		g.close();
		
		TitanGraph graph = GraphOfTheGodsFactory.create();
//		load(graph);
		query(graph);
		graph.close();
	}
	
}
