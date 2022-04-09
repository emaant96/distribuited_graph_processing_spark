package SparkProject.graphWork;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.graphx.lib.ShortestPaths;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Map;
import scala.collection.Seq;
import scala.reflect.ClassTag;

public class PreImplemented_method {

	ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

	public void doPageRank(Graph<String, String> graph, int n) {

		System.out.println("Command accepted \n");
		Graph<Object, Object> pageRank = PageRank.run(graph, 50, 0.0001, graph.vertices().vdTag(),
				graph.vertices().vdTag());

		VertexRDD<Object> vertexRDD = pageRank.vertices();

		List<Tuple2<Object, Object>> sortedList = vertexRDD.toJavaRDD().takeOrdered(n, SerializableComparator
				.serialize((a, b) -> -1 * new comp1().compare(100 * (Double) a._2(), 100 * (Double) b._2())));

		HashMap<Long, Object> hashSortedList = new HashMap<Long, Object>();
		
		for (Tuple2<Object, Object> tuple : sortedList)
			hashSortedList.put((Long) tuple._1(), tuple._2());

		Function<Tuple2<Object, String>, Boolean> filter = tuple -> (hashSortedList.containsKey(tuple._1()));
		List<Tuple2<Object, String>> listvert = graph.vertices().toJavaRDD().filter(filter).collect();
		
		HashMap<Long, String> titleConvList = new HashMap<Long, String>();
		
		for (Tuple2<Object, String> vert : listvert)
			titleConvList.put((Long) vert._1(), vert._2());


		for (Tuple2<Object, Object> tuple : sortedList)
			System.out.println(titleConvList.get(tuple._1()) + "(id:" + tuple._1() + ")" + ": " + tuple._2());
		System.out.println("\n");
	}

	public void doShortestPath(Graph<String, String> graph, List<Object> destVertices, Object v1, String[] titles, Long[] ids) {
		System.out.println("Command accepted \n");

		Seq<Object> destVert = JavaConverters.asScalaBuffer(destVertices).toList().toSeq();
		Graph<Map<Object, Object>, String> shortestPath = ShortestPaths.run(graph, destVert, stringTag);
		VertexRDD<Map<Object, Object>> vertexRDD = shortestPath.vertices();
		Function<Tuple2<Object, Map<Object, Object>>, Boolean> filter = idVertex -> (Long.toString((Long) idVertex._1())).equals(Long.toString((Long) v1));
		JavaRDD<Tuple2<Object, Map<Object, Object>>> vertexRDDfiltered = vertexRDD.toJavaRDD().filter(filter);

		Iterator<Tuple2<Object, Object>> listPath = vertexRDDfiltered.first()._2().iterator();

		if (listPath.hasNext()) {
			System.out.println("Shortest Path for " + titles[0] +"\n");

			HashMap<Long, String> map = new HashMap<Long, String>();

			for (int j = 0; j < titles.length; j++)
				map.put(ids[j], titles[j]);

			Tuple2<Object, Object> tuple;

			while (listPath.hasNext()) {
				tuple = listPath.next();
				System.out.println(map.get((Long) tuple._1()) + "(id:" + String.valueOf(tuple._1()) + ")" + "->"
						+ String.valueOf(tuple._2()));
			}
			System.out.println("\n");
		} else {
			System.out.println("Current graph does not contain any of destination nodes \n"
								+ "or there isn't any path from source to destinations \n");
		}
	}

	public void doConnectedComponents(Graph<String, String> graph) {
		System.out.println("Command accepted \n");
		Graph<Object, String> connComp = ConnectedComponents.run(graph, stringTag, stringTag);

		Function<Tuple2<Object, Object>, Tuple2<Object, Object>> mapper = (vert) -> {
			return new Tuple2<Object, Object>(vert._2(), vert._2());
		};
			
		JavaRDD<Tuple2<Object, Object>> res = connComp.vertices().toJavaRDD().map(mapper).distinct();
		System.out.println("Graph with:" + res.count() + " connected components \n");
	}

	public interface SerializableComparator<T> extends Comparator<T>, Serializable {

		static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
			return comparator;
		}
	}
}
