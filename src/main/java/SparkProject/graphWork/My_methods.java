package SparkProject.graphWork;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class My_methods {

	ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

	public Graph<String, String> removeEdgesWithoutChild(Graph<String, String> graph, int n) {
		System.out.println("Command accepted");
		GraphOps<String, String> graphOps = Graph.graphToGraphOps(graph, stringTag, stringTag);
		Function<Tuple2<Object, Object>, Boolean> filter = t -> ((int) t._2() > n);

		Function<Tuple2<Object, Object>, Long> mapper = t -> ((Long) t._1());
		List<Long> outVers = graphOps.outDegrees().toJavaRDD().filter(filter).map(mapper).collect();
		
		Function<Tuple2<Object, String>, Boolean> filterNodes = t -> (outVers.contains((Long) t._1()));
		Function<Edge<String>, Boolean> filterEdges = t -> (outVers.contains(t.srcId()) && outVers.contains(t.dstId()));
		RDD<Tuple2<Object, String>> verticesFiltered = graph.vertices().toJavaRDD().filter(filterNodes).rdd();
		RDD<Edge<String>> edgesFiltered = graph.edges().toJavaRDD().filter(filterEdges).rdd();

		System.out.println("Filtering vertices....");
		System.out.println("Number of vertices: " + verticesFiltered.count());

		System.out.println("Filtering edges....");
		System.out.println("Number of edges: " + edgesFiltered.count());
		
		System.out.println("Graph Filtered \n");
		
		return Graph.apply(verticesFiltered, edgesFiltered, " ", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
				stringTag, stringTag);
	}

	public void saveGraphAsCSV(Graph<String, String> graph, String name) throws IOException {
		System.out.println("Command accepted \n");
		File file = new File("Graphs/" + name);

		if (file.mkdir()) {

			BufferedWriter bw = new BufferedWriter(new FileWriter("Graphs/" + name + "/vertices.csv"));

			List<Tuple2<Object, String>> vertices = graph.vertices().toJavaRDD().collect();
			System.out.println("Saving vertices");
			for (Tuple2<Object, String> vertex : vertices)
				bw.append(String.valueOf(vertex._1()) + ";" + vertex._2() + "\n");

			bw.flush();
			bw.close();

			BufferedWriter bw1 = new BufferedWriter(new FileWriter("Graphs/" + name + "/edges.csv"));

			List<Edge<String>> edges = graph.edges().toJavaRDD().collect();
			System.out.println("Saving edges");
			for (Edge<String> edge : edges)
				bw1.append(String.valueOf(edge.srcId()) + ";" + String.valueOf(edge.dstId()) + "\n");

			bw1.flush();
			bw1.close();
			System.out.println("Done \n");
		} else {
			System.out.println("Graph: " + name + " already exist \n");
		}
	}

	public void saveGraphAsRDD(Graph<String, String> graph, String name) {
		graph.vertices().saveAsTextFile("Graphs/" + name + "/vertices");
		graph.edges().saveAsTextFile("Graphs/" + name + "/edges");
	}
	
	public Graph<String, String> loadGraphFromCSV(JavaSparkContext javaSparkContext, String url) {
		System.out.println("Command Accepted \n");
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

		Function<String, Edge<String>> edgConv = text -> {
			String[] tuple = text.split(";");
			return new Edge<String>(Long.parseLong(tuple[0]), Long.parseLong(tuple[1]), "Link");
		};
		Function<String, Tuple2<Object, String>> vxConv = text -> {
			String[] tuple = text.split(";");
			return new Tuple2<Object, String>(Long.parseLong(tuple[0]), tuple[1]);
		};
		JavaRDD<Edge<String>> edgeRDD;
		JavaRDD<Tuple2<Object, String>> vertexRDD;
		Graph<String, String> graph;
		
		try {
			edgeRDD = javaSparkContext.textFile(url + "/edges.csv").map(edgConv);
			vertexRDD = javaSparkContext.textFile(url + "/vertices.csv").map(vxConv);
			graph = Graph.apply(vertexRDD.rdd(), edgeRDD.rdd(), " ", StorageLevel.MEMORY_ONLY(),
					StorageLevel.MEMORY_ONLY(), stringTag, stringTag);
		} catch (Exception e) {
			return null;
		}
		return graph;
	}

	public Long[] getVertexId(Graph<String, String> graph, String[] titoli) {
		int i = 0;
		Long[] ids = new Long[titoli.length];
		for (String titolo : titoli) {
			Function<Tuple2<Object, String>, Boolean> findVert = tuple -> (String.valueOf(tuple._2()).equals(titolo));
			JavaRDD<Tuple2<Object, String>> listTitoli = graph.vertices().toJavaRDD().filter(findVert);

			if (!listTitoli.isEmpty())
				ids[i] = (Long) listTitoli.first()._1();
			else {
				if (i == 0)
					return null;
			}
			i++;
		}
		return ids;
	}

}
