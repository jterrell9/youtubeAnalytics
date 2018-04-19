import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.primitives.UnsignedLong;

import main.java.util.JSONparser;
import main.java.util.spark.CategoryCount.CSVLineParser;
import scala.Tuple2;

//[0] "video_id",
//[1] "trending_date",
//[2] "title",
//[3] "channel_title",
//[4] "category_id",
//[5] "publish_time",
//[6] "tags",
//[7] "views",
//[8] "likes",
//[9] "dislikes",
//[10] "comment_count",
//[11] "thumbnail_link",
//[12] "comments_disabled",
//[13] "ratings_disabled",
//[14] "video_error_or_removed",
//[15] "description"

@SuppressWarnings("serial")
public class Driver {
	private static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
	private static final String csvHeader = "title,category_id:category,publish_date,trending_date,channel_title,views,likes,dislikes,URL";
	private static String inputDir;
	private static final String welcome = "\n****************************************"
			+ "\n~~~~~~~~YouTube Analytics Suite~~~~~~~~~"
			+ "\n  Copyright April 2018 by Jack Terrell"
			+ "\n****************************************";
	private static final String menu = "\n                MAIN MENU"
			+ "\n****************************************"
			+ "\n1: Choose Categories"
			+ "\n2: Count Videos by Categories"
			+ "\n3: Filter Videos by Categories"
			+ "\n4: Videos with Top K Likes"
			+ "\n5: Videos with Top K Dislikes"
			+ "\n6: Videos with Top K Views separated by category"
			+ "\n7: Videos with Top K Views of all categories"
			+ "\n8: Videos with Top K Likes of all categories"
			+ "\n9: Videos with Top K Dislikes of all categories"
			+ "\n10: Videos with Top K views of all categories of videos from 2018"
			+ "\n11: Videos with Top K likes of all categories of videos from 2018"
			+ "\n12: Videos with Top K dislikes of all categories of videos from 2018"
			+ "\n13: View Selected Categories"
			+ "\n14: Quit"
			+ "\n****************************************";
	private static String[] category_ids = null;
	private static String category_id = null;
	private static String categoryStr = null;
	private static int k = 0;
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.out.println(welcome);
		Scanner user = new Scanner(System.in);
		System.out.print("Be sure to choose categories first"
				+ "\n\nPath to \"USvideos.csv\" >> hdfs://localhost:9000/");
		if(user.hasNext()) inputDir = user.next();
		else {
			user.close();
			throw new RuntimeException("invalid input path");
		}
		inputDir = "hdfs://localhost:9000/" + inputDir + "/USvideos.csv";
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("YouTube Analytics Suite");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		boolean loop;
		do {
			loop = true;
			System.out.println(menu);
			System.out.print("Enter selection >> ");
			int selection = user.nextInt();
			switch(selection) {
			case(1):	////Choose Categories
				System.out.println("id\t|category name:"
						+ "\n------------------------"
						+ "\n" + JSONparser.getCategory(JSONparser.LIST_ALL));
				System.out.print("How many categories (enter 0 for all categories): ");
				int n = 0;
				if(user.hasNextInt()) n = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				category_ids = chooseCategories(n);
				System.out.println("\nCategories added: ");
				for(int i=0; i<category_ids.length; i++) {
					System.out.print((i+1) + ": " + JSONparser.getCategory(category_ids[i]) + "\n");
				}
				break;
			case(2):	//Count Videos by Categories
				//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,
				//[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
				JavaPairRDD<String, String> inputRDD2 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				Map<String, Long> categoryCountMap = new HashMap<String, Long>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					Long count = inputRDD2.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).count();
					categoryCountMap.put(JSONparser.getCategory(category_id), count);
				}
				String category;
				for(int i=0; i<category_ids.length; i++) {
					category = JSONparser.getCategory(category_ids[i]);
					System.out.println(">>Total number of videos in the \"" + category + "\" category: " + categoryCountMap.get(category));
				}
				category_id = null;
				categoryStr = null;
				break;
			case(3):	//Filter Videos by Categories
				JavaPairRDD<String, String> inputRDD3 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				List<String> list3 = null;
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list3 = inputRDD3.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String views = split[5];
							Integer viewsInt = Integer.parseInt(views);
							return new Tuple2<Integer, String>(viewsInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).sortByKey(false).values().collect();
					System.out.println(categoryStr + " filter: "
							+ "\n" + list3.toString());
					writeListToCSV(list3, csvHeader, categoryStr);
					if(list3 != null) {
						for(@SuppressWarnings("unused") String value : list3) {
							System.out.println(">> number of \"" + categoryStr + "\" videos: " + list3.size());
						}
					}
					list3 = null;
				}
				category_id = null;
				categoryStr = null;
				break;
			case(4):	//Videos with Top K Likes
				JavaPairRDD<String, String> inputRDD4 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.println("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<String> list4 = null;
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list4 = inputRDD4.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String likes = split[6];
							Integer likesInt = Integer.parseInt(likes);
							return new Tuple2<Integer, String>(likesInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).sortByKey(false).values().take(k);
					System.out.println(categoryStr + " filter: "
							+ "\n" + list4.toString());
					writeListToCSV(list4, csvHeader, categoryStr);
					list4 = null;
				}
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(5):	//Videos with Top K Dislikes
				JavaPairRDD<String, String> inputRDD5 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<String> list5 = null;
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list5 = inputRDD5.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String dislikes = split[7];
							Integer dislikesInt = Integer.parseInt(dislikes);
							return new Tuple2<Integer, String>(dislikesInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).sortByKey(false).values().take(k);
					writeListToCSV(list5, csvHeader, categoryStr);
					list5 = null;
				}
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(6):	//Videos with Top K Views separated by category
				JavaPairRDD<String, String> inputRDD6 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<String> list6 = null;
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list6 = inputRDD6.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String views = split[5];
							Integer viewsInt = Integer.parseInt(views);
							return new Tuple2<Integer, String>(viewsInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).sortByKey(false).values().take(k);
					writeListToCSV(list6, csvHeader, categoryStr);
					list6 = null;
				}
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(7):	//Videos with Top K Views of all categories
				JavaPairRDD<String, String> inputRDD7 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Integer, String>> list7 = new ArrayList<Tuple2<Integer, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list7.addAll(inputRDD7.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,
							//[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String views = split[5];
							Integer viewsInt = Integer.parseInt(views);
							return new Tuple2<Integer, String>(viewsInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList7 = sparkContext.parallelize(list7)
						.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<Integer, String> record) throws Exception {
						return new Tuple2<Integer, String>(record._1(), record._2());
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList7, csvHeader, "top" + k + "views");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(8):	//Videos with Top K Likes of all categories
				JavaPairRDD<String, String> inputRDD8 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Integer, String>> list8 = new ArrayList<Tuple2<Integer, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list8.addAll(inputRDD8.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String likes = split[6];
							Integer likesInt = Integer.parseInt(likes);
							return new Tuple2<Integer, String>(likesInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList8 = sparkContext.parallelize(list8).mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<Integer, String> record) throws Exception {
						return new Tuple2<Integer, String>(record._1(), record._2());
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList8, csvHeader, "top" + k + "likes");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(9):	//Videos with Top K dislikes of all categories
				JavaPairRDD<String, String> inputRDD9 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Integer, String>> list9 = new ArrayList<Tuple2<Integer, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list9.addAll(inputRDD9.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Integer, String>() {
						@Override
						public Tuple2<Integer, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String dislikes = split[7];
							Integer dislikesInt = Integer.parseInt(dislikes);
							return new Tuple2<Integer, String>(dislikesInt, record);
						}
					}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Integer, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList9 = sparkContext.parallelize(list9).mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<Integer, String> record) throws Exception {
						return new Tuple2<Integer, String>(record._1(), record._2());
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList9, csvHeader, "top" + k + "dislikes");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(10):	//Videos with Top K views of all categories of videos from 2018
				JavaPairRDD<String, String> inputRDD10 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Tuple2<Integer, String>, String>> list10 = new ArrayList<Tuple2<Tuple2<Integer, String>, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list10.addAll(inputRDD10.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Tuple2<Integer, String>, String>() {
						@Override
						public Tuple2<Tuple2<Integer, String>, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String views = split[5];
							Integer viewsInt = Integer.parseInt(views);
							String publish_date = split[2];
							return new Tuple2<Tuple2<Integer, String>, String>(new Tuple2<Integer, String>(viewsInt, publish_date), record);
						}
					}).filter(new Function<Tuple2<Tuple2<Integer, String>, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList10 = sparkContext.parallelize(list10).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, String>, String>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
						return new Tuple2<String, String>(record._1()._2(), record._2());
					}
				}).filter(new Function<Tuple2<String, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, String> record) throws Exception {
						if(record._1().subSequence(0, 4).equals("2018")) return true;
						return false;
					}
				}).values().mapToPair(new PairFunction<String, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(String record) throws Exception {
						//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
						String[] split = CSVLineParser.splitLine2array(record);
						if(split.length != 9) return null;
						String views = split[5];
						Integer viewsInt = null;
						try {
							viewsInt = Integer.parseInt(views);
						}catch(NumberFormatException nfe) {
							return null;
						}
						return new Tuple2<Integer, String>(viewsInt, record);
					}
				}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<Integer, String> record) throws Exception {
						if(record == null) return false;
						return true;
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList10, csvHeader, "top" + k + "viewsOf2018");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(11):	//Videos with Top K likes of all categories of videos from 2018
				JavaPairRDD<String, String> inputRDD11 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Tuple2<Integer, String>, String>> list11 = new ArrayList<Tuple2<Tuple2<Integer, String>, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list11.addAll(inputRDD11.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Tuple2<Integer, String>, String>() {
						@Override
						public Tuple2<Tuple2<Integer, String>, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String likes = split[6];
							Integer likesInt = null;
							try {
								likesInt = Integer.parseInt(likes);
							}catch(NumberFormatException nfe) {
								return null;
							}
							String publish_date = split[2];
							return new Tuple2<Tuple2<Integer, String>, String>(new Tuple2<Integer, String>(likesInt, publish_date), record);
						}
					}).filter(new Function<Tuple2<Tuple2<Integer, String>, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList11 = sparkContext.parallelize(list11).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, String>, String>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
						return new Tuple2<String, String>(record._1()._2(), record._2());
					}
				}).filter(new Function<Tuple2<String, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, String> record) throws Exception {
						if(record._1().subSequence(0, 4).equals("2018")) return true;
						return false;
					}
				}).values().mapToPair(new PairFunction<String, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(String record) throws Exception {
						//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
						String[] split = CSVLineParser.splitLine2array(record);
						if(split.length != 9) return null;
						String likes = split[6];
						Integer likesInt = null;
						try {
							likesInt = Integer.parseInt(likes);
						}catch(NumberFormatException nfe) {
							return null;
						}
						return new Tuple2<Integer, String>(likesInt, record);
					}
				}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<Integer, String> record) throws Exception {
						if(record == null) return false;
						return true;
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList11, csvHeader, "top" + k + "likesOf2018");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(12):	//Videos with Top K dislikes of all categories of videos from 2018
				JavaPairRDD<String, String> inputRDD12 = createRDD(inputDir, sparkContext);
				if(category_ids == null) {
					System.err.println(">>please select categories");
					loop = true;
					break;
				}
				System.out.print("How many records would you like to output (k): ");
				if(user.hasNextInt()) k = user.nextInt();
				else {
					System.err.println("invalid input, use whole number");
					break;
				}
				List<Tuple2<Tuple2<Integer, String>, String>> list12 = new ArrayList<Tuple2<Tuple2<Integer, String>, String>>();
				for(int i=0; i<category_ids.length; i++) {
					category_id = category_ids[i];
					categoryStr = JSONparser.getCategory(category_id);
					list12.addAll(inputRDD12.filter(new Function<Tuple2<String, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<String, String> record) throws Exception {
							return record._1().equals(category_id);
						}
					}).values().mapToPair(new PairFunction<String, Tuple2<Integer, String>, String>() {
						@Override
						public Tuple2<Tuple2<Integer, String>, String> call(String record) throws Exception {
							//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
							String[] split = CSVLineParser.splitLine2array(record);
							if(split.length != 9) return null;
							String dislikes = split[7];
							Integer dislikesInt = Integer.parseInt(dislikes);
							String publish_date = split[2];
							return new Tuple2<Tuple2<Integer, String>, String>(new Tuple2<Integer, String>(dislikesInt, publish_date), record);
						}
					}).filter(new Function<Tuple2<Tuple2<Integer, String>, String>, Boolean>() {
						@Override
						public Boolean call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
							if(record == null) return false;
							return true;
						}
					}).collect());
				}
				
				List<String> rddList12 = sparkContext.parallelize(list12).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, String>, String>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<Tuple2<Integer, String>, String> record) throws Exception {
						return new Tuple2<String, String>(record._1()._2(), record._2());
					}
				}).filter(new Function<Tuple2<String, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, String> record) throws Exception {
						if(record._1().subSequence(0, 4).equals("2018")) return true;
						return false;
					}
				}).values().mapToPair(new PairFunction<String, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(String record) throws Exception {
						//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
						String[] split = CSVLineParser.splitLine2array(record);
						if(split.length != 9) return null;
						String dislikes = split[7];
						Integer dislikesInt = null;
						try {
							dislikesInt = Integer.parseInt(dislikes);
						}catch(NumberFormatException nfe) {
							return null;
						}
						return new Tuple2<Integer, String>(dislikesInt, record);
					}
				}).filter(new Function<Tuple2<Integer, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<Integer, String> record) throws Exception {
						if(record == null) return false;
						return true;
					}
				}).sortByKey(false).values().take(k);
				writeListToCSV(rddList12, csvHeader, "top" + k + "dislikesOf2018");
				category_id = null;
				categoryStr = null;
				k = 0;
				break;
			case(13): 	//View Selected Categories
				System.out.println("\nSelected Categories: ");
				for(int i=0; i<category_ids.length; i++) {
					System.out.print((i+1) + ": " + JSONparser.getCategory(category_ids[i]) + "\n");
				}
				break;
			case(14):
				System.out.println("\n****************************************"
						+ "\n~~~~~~~~~~~YouTube Analytics Suite~~~~~~"
						+ "\n  Copyright April 2018 by Jack Terrell"
						+ "\n~~~~~~~~~~~~~~~~Goodbye~~~~~~~~~~~~~~~~~"
						+ "\n****************************************");
				sparkContext.close();
				System.exit(0);
			default:
				System.err.println("invalid selection");
				loop = true;
			}
		}while(loop);
		sparkContext.close();
	}
	private static JavaPairRDD<String, String> createRDD(String inputDir, JavaSparkContext context) {
		JavaPairRDD<String, String> rdd = context.textFile(inputDir).mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String record) throws Exception {
				if(record.equals(firtLine)) {
					System.err.println("\n>>reading file..."); 
					return null; 
				}
				record = record.replaceAll("\\n", " ");
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length != 16) return null;
				String title = "\"" + split[2] + "\"";
				String publish_time = split[5];
				if(publish_time.length() < 10 || publish_time.charAt(0) != '2') return null;
				String publish_date = publish_time.substring(0, 10);
				String category_id = split[4];
				String category = JSONparser.getCategory(category_id);
				String trending_date = split[1];
				String channel_title = split[3];
				String views = split[7];
				String likes = split[8];
				String dislikes = split[9];
				String video_id = split[0];
				String URL = "https://www.youtube.com/watch?v=" + video_id;
				String csvLine = title + "," + category_id + ":" + category + "," + publish_date + "," 
								+ trending_date + "," + channel_title + "," + views + "," + likes + "," + dislikes + "," + URL;
				return new Tuple2<String, String>(video_id, csvLine);
			}
		//[0]title,[1]category_id:category,[2]publish_date,[3]trending_date,[4]channel_title,[5]views,[6]likes,[7]dislikes,[8]URL
		}).filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				if(record == null) return false;
				return true;
			}
		}).reduceByKey(new Function2<String, String, String>() {
			@SuppressWarnings("deprecation")
			@Override
			public String call(String record1, String record2) throws Exception {
				String[] split1 = CSVLineParser.splitLine2array(record1);
				String[] split2 = CSVLineParser.splitLine2array(record2);
				if(split1.length != 9 || split2.length != 9) return "";
				UnsignedLong viewsCount = null;
				UnsignedLong likesCount = null;
				UnsignedLong dislikesCount = null;
				try {
					String views1 =  split1[5];
					String views2 = split2[5];
					UnsignedLong viewsCount1 = UnsignedLong.valueOf(views1);
					UnsignedLong viewsCount2 = UnsignedLong.valueOf(views2);
					viewsCount = viewsCount1.add(viewsCount2);
					String likes1 = split1[6];
					String likes2 = split2[6];
					UnsignedLong likesCount1 = UnsignedLong.valueOf(likes1);
					UnsignedLong likesCount2 = UnsignedLong.valueOf(likes2);
					likesCount = likesCount1.add(likesCount2);
					String dislikes1 = split1[7];
					String dislikes2 = split2[7];
					UnsignedLong dislikesCount1 = UnsignedLong.valueOf(dislikes1);
					UnsignedLong dislikesCount2 = UnsignedLong.valueOf(dislikes2);
					dislikesCount = dislikesCount1.add(dislikesCount2);
				}catch(NumberFormatException nfe) {
					return "";
				}
				return split1[0] + "," + split1[1] + "," + split1[2] + "," + split1[3] + "," 
						+ split1[4] + "," +  viewsCount + "," + likesCount + "," + dislikesCount + "," + split1[8];
			}
		}).values().mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String record) throws Exception {
				if(record == null || record.equals("")) return null;
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length != 9) return null;
				String category_id = split[1].split(":")[0].trim();
				return new Tuple2<String, String>(category_id, record);
			}
		}).filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				if(record == null) return false;
				return true;
			}
		});
		return rdd;
	}
	
	private static void writeListToCSV(List<String> list, String headers, String title) {
		FileWriter output;
		try {
			output = new FileWriter(new File("./output/" + title + ".csv"));
			output.write(headers + "\n");
			for(String record : list) {
				output.write(record + "\n");
			}
			output.close();
		}catch(IOException ioe) {
			System.err.println("ERROR: file not found");
			System.exit(2);
		}
		System.out.println(">>file \"" + title + ".csv\" created in output directory");
	}
	
	@SuppressWarnings("resource")
	private static String[] chooseCategories(int n) {
		if(n == 0) return getAllCategories();
		String[] category_ids = new String[n];
		Scanner selection = new Scanner(System.in);
		for(int i=0; i<n; i++) {
			System.out.print("category " + (i+1) + ": ");
			String category_id;
			String category;
			if(selection.hasNextInt()) category_id = selection.next();
			else {
				System.err.println("invalid input, category ID should be whole number");
				i--;
				continue;
			}
			category = JSONparser.getCategory(category_id);
			if(category == null) {
				System.err.println("no category " + category_id);
				i--;
				continue;
			}
			category_ids[i] = category_id;
			System.out.println(category + " catgeory added");
		}
		return category_ids;
	}
	
	private static String[] getAllCategories() {
		String allCategoriesStr = JSONparser.getCategory(JSONparser.LIST_ALL);
		String[] lines = allCategoriesStr.split("\n");
		String[] category_ids = new String[lines.length];
		for(int i=0; i<category_ids.length; i++) {
			category_ids[i] = lines[i].split("\t")[0].trim();
		}
		return category_ids;
	}
}
