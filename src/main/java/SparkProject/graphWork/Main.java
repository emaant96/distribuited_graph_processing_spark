package SparkProject.graphWork;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;

public class Main {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache").setLevel(Level.OFF);
		final SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("grafExample");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		console(br, javaSparkContext);

		javaSparkContext.close();
		br.close();

	}

	public static void console(BufferedReader br, JavaSparkContext javaSparkContext) throws Exception {
		String s = "";
		System.out.println("\n Ready to load a graph." + " Try Graphs/wikiGraph" + "\n" + "Type HELP for info \n");
		Graph<String, String> graph = null;

		while (!(s = br.readLine()).contentEquals("STOP")) {

			String[] s1 = s.split(" ");

			if (s1[0].equals("LOAD") && s1.length == 2) {
				graph = new My_methods().loadGraphFromCSV(javaSparkContext, s1[1]);
				if (graph != null)
					System.out.println("Graph correctly loaded \n");
				else
					System.out.println("File not founded \n");
			}

			if (s1[0].equals("SHORTESTPATH") && graph != null && s1.length > 2) {
				Long[] ids = new My_methods().getVertexId(graph, Arrays.copyOfRange(s1, 1, s1.length));
				if (ids != null) {
					
					List<Object> destList = new ArrayList<Object>();
					Object source = (Object) ids[0];
					
					for (int i = 1; i < ids.length; i++)
						destList.add((Object) ids[i]);

					new PreImplemented_method().doShortestPath(graph, destList, source,
							Arrays.copyOfRange(s1, 1, s1.length), ids);
				} else
					System.out.println("Current graph doesn't contain Source Node: " + s1[1]);
			}

			if (s1[0].equals("PAGERANK") && graph != null && s1.length == 2)
				new PreImplemented_method().doPageRank(graph, Integer.parseInt(s1[1]));

			if (s1[0].equals("CONNCOMPONENTS") && graph != null)
				new PreImplemented_method().doConnectedComponents(graph);

			if (s1[0].equals("REMOVEVERTICES") && graph != null && s1.length == 2)
				graph = new My_methods().removeEdgesWithoutChild(graph, Integer.parseInt(s1[1]));
			
			if (s1[0].equals("SAVE") && graph != null && s1.length == 3) {
				if (s1[1].equals("CSV"))
					new My_methods().saveGraphAsCSV(graph, s1[2]);
				if (s1[1].equals("RDD"))
					new My_methods().saveGraphAsRDD(graph, s1[2]);
			}

			if (s1[0].equals("HELP")) {
				System.out.println("Possible commands:" + "\n" 
						+ "		LOAD <path>" + "\n"
						+ "		SHORTESTPATH <source> <nodeDestination1>....<nodeDestinationN>" + "\n"
						+ "		PAGERANK <numEntryResults>" + "\n" 
						+ "		CONNCOMPONENTS" + "\n"
						+ "		REMOVEVERTICES <numchild>" + "\n" 
						+ "		SAVE <RDD/CSV> <nameGraph> \n");
			}
		}
		System.out.println("Correctly Stopped");
	}
}