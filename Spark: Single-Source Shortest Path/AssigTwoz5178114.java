/*
Spark: Single-Source Shortest Path
Created by Rongtao Shen on 15/11/2019
Student ID: z5178114
In this assignment, I firstly get the starting node from the command line and define a string to store the starting node.
Then, I create an informationRDD by reading the each line of the input textfile.
In the input textfile, every line is the information of the edges in graph, which contains starting point, end point and weight of this edge.
In the informationRDD, I set the starting point as the key and set the tuple of end point and weight as the value.
After that, I get the whole information of the graph. In addition, I creat an ArrayList to store all the nodes in graph.
Next, I generate another RDD called adjacencyRDD which stores the paths of the neighbor node of the starting node.
Then, I use a while loop to join the adjacancyRDD and the informationRDD. For every join operation, I will update the adjacancyRDD.
In addition, I will store the newRDD into the allPath by using the union method until all the paths in the graph are found.
In the worst case, it requires #n-1 loop, where n is the number of the nodes in graph, which I can get from the size of the ArrayList of all nodes.
After getting all the paths in the graph, I use reduceByKey() method to get the shortest paths of every nodes in graph except the starting node
by comparing the cost of paths with the same key.
Finally, I get all the shortest paths and store them in PathNodeRDD.
Next step, I need to handle the nodes which do not have the path.
Firstly, I create an ArrayList called PathNode to store the nodes which have the shortest paths.
Then I traverse the ArrayList of all nodes to judge the node whether it is in the ArrayList(PathNode) above,
which I can get the ArrayList of nodes which do not have the path and then I convert this ArrayList to RDD called NoPathNodeRDD.
Finally, I use union method to put PathNodeRDD and NoPathNodeRDD together and save as a text file.
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class AssigTwoz5178114 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Assignment 2").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
	//get the starting node from the command line
        String starting_node = args[0];
	//get the input file from the command line
        JavaRDD<String> input = context.textFile(args[1]);
        //create a RDD called informationRDD to store the information of the graph
        JavaPairRDD<String, Tuple2<String, Integer>> informationRDD = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
                String [] parts = line.split(",");
                String first_node = parts[0];
                String second_node = parts[1];
                Integer distance = Integer.parseInt(parts[2]);
                return new Tuple2<String, Tuple2<String, Integer>>(first_node, new Tuple2<>(second_node, distance));
            }
        });
        //create a ArrayList called nodes to store all nodes in graph
	ArrayList<String> nodes = new ArrayList<String>();
        informationRDD.groupByKey().keys().collect().forEach(nodes::add);
        ArrayList<Tuple2<String, Integer>> valueList = new ArrayList<Tuple2<String, Integer>>(informationRDD.values().collect());
        for (int i = 0; i < valueList.size(); i++) {
            if(!nodes.contains(valueList.get(i)._1)) {
                nodes.add(valueList.get(i)._1);
            }
        }
	//set the loop number
        Integer count = nodes.size() - 1;
	//create a RDD called adjacencyRDD to store the paths of neighbour nodes of the starting node
        JavaPairRDD<String, Tuple2<String, Integer>> adjacencyRDD = informationRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
                ArrayList<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
                if (input._1.equals(starting_node)) {
                    String new_start = input._2._1;
                    StringBuilder sb = new StringBuilder();
                    sb.append(input._1).append('-').append(input._2._1);
                    String path = sb.toString();
                    Integer distance = input._2._2;
                    list.add(new Tuple2<String, Tuple2<String, Integer>>(new_start, new Tuple2<>(path, distance)));
                }
                return list.iterator();
            }
        });
	//create a RDD called allPath to store all paths in graph
        JavaPairRDD<String, Tuple2<String, Integer>> allPath = adjacencyRDD;
        JavaPairRDD<String, Tuple2<String, Integer>> joinRDD = adjacencyRDD;
        //get all paths in graph
        while (count != 0) {
            JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> combinedRDD = joinRDD.join(informationRDD);
            JavaPairRDD<String, Tuple2<String, Integer>> newRDD = combinedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>>, String, Tuple2<String, Integer>>() {
                @Override
                public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> input) throws Exception {
                    ArrayList<Tuple2<String, Tuple2<String, Integer>>> path_list = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
                    if (!input._2._1._1.contains(input._2._2._1)) {
                        StringBuilder sb = new StringBuilder();
                        String key = input._2._2._1;
                        sb.append(input._2._1._1).append('-').append(input._2._2._1);
                        String path = sb.toString();
                        Integer cost = input._2._1._2 + input._2._2._2;
                        path_list.add(new Tuple2<String, Tuple2<String, Integer>>(key, new Tuple2<>(path, cost)));
                    }
                    return path_list.iterator();
                }
            });
            allPath = allPath.union(newRDD);
            joinRDD = newRDD;
            count = count - 1;
        }
	//get shortest paths in graph
        JavaPairRDD<String, Tuple2<String, Integer>> shortest_path = allPath.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                Tuple2<String, Integer> shortest_path;
                if (v1._2 < v2._2) {
                    shortest_path = v1;
                }
                else {
                    shortest_path = v2;
                }
                return shortest_path;
            }
        });
	//sorted by length of shortest path
        JavaPairRDD<Integer, Tuple2<String, String>> sortRDD = shortest_path.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, Integer, Tuple2<String, String>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
                Integer distance = input._2._2;
                String destination = input._1;
                String path = input._2._1;
                return new Tuple2<Integer, Tuple2<String, String>>(distance, new Tuple2<>(destination, path));
            }
        });
	//set format of output
        JavaPairRDD<String, Tuple2<Integer, String>> PathNodeRDD = sortRDD.sortByKey().mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, String>>, String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, String>> call(Tuple2<Integer, Tuple2<String, String>> input) throws Exception {
                String destination = input._2._1;
                Integer distance = input._1;
                String path = input._2._2;
                return new Tuple2<String, Tuple2<Integer, String>>(destination, new Tuple2<>(distance, path));
            }
        });
	//create a ArrayList called PathNode to store nodes which have the shortest path
        ArrayList<String> PathNode = new ArrayList<String>(PathNodeRDD.keys().collect());
        ArrayList<Tuple2<String, Tuple2<Integer, String>>> NoPathNodes = new ArrayList<Tuple2<String, Tuple2<Integer, String>>>();
        for (int j = 0; j < nodes.size(); j++) {
            if (!nodes.get(j).equals(starting_node) && !PathNode.contains(nodes.get(j))) {
                NoPathNodes.add(new Tuple2<>(nodes.get(j), new Tuple2<>(-1, "")));
            }
        }
	//handle the nodes which do not have the path
        JavaPairRDD<String, Tuple2<Integer, String>> NoPathNodeRDD = context.parallelizePairs(NoPathNodes);
        JavaRDD<String> output = PathNodeRDD.union(NoPathNodeRDD).map(new Function<Tuple2<String, Tuple2<Integer, String>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<Integer, String>> input) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(input._1).append(',').append(input._2._1).append(',').append(input._2._2);
                return sb.toString();
            }
        });
	//save output as a textfile
        output.coalesce(1, true).saveAsTextFile(args[2]);
    }
}
