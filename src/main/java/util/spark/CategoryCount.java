package main.java.util.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.primitives.UnsignedLong;

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

//category-ids
//10	Music
//27	Education
//24	Entertainment
//28	Science & Technology
//25	News & Politics

public class CategoryCount {
	public static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	public static int skipCount = 0;
	public static UnsignedLong k = null;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//check correct usage
		if(args.length != 1) {
			System.err.println("usage: $SPARK-HOME/bin/spark-submit --class CategoryCount ./target/<jar-file> <input-dir>"
					+ "\nonly specify working path (the directory) of file within the hdfs");
			System.exit(1);
		}
		//grab input directories
		String inputDir = args[0];
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("Filter by category_id, sort by views");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaPairRDD<String, String> categoryRDD = sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/USvideos.csv")
				.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String record) throws Exception {
				if(record.equals(firtLine)) {
					System.err.print("*	reading file..."); 
					return skip(); 
				}
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length < 16) return skip();
				String category_id = split[4];
				if(category_id == null  || category_id.equals("") || record == null) return skip();
				return new Tuple2<String, String>(category_id, record);
			}	
		}).filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> skipValue) throws Exception {
				if(skipValue == null || skipValue._1() == null || skipValue._2() == null) return false;
				return true;
			}
		}).persist(StorageLevel.MEMORY_AND_DISK());
		
		JavaRDD<String> musicRDD = categoryRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				return record._1().equals("10");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> educationRDD = categoryRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				return record._1().equals("27");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> entertainmentRDD = categoryRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				return record._1().equals("24");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> scienceAndTechRDD = categoryRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				return record._1().equals("28");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> newsAndPoliticsRDD = categoryRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) throws Exception {
				return record._1().equals("25");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		
		Long musicCount = musicRDD.count();
		Long educationCount = educationRDD.count();
		Long entertainmentCount = entertainmentRDD.count();
		Long SandTCount = scienceAndTechRDD.count();
		Long NandPCount = newsAndPoliticsRDD.count();
		Long totalCount = musicCount + educationCount + entertainmentCount + SandTCount + NandPCount;
		
		Long recordCount = categoryRDD.count();
		
		//print out record counts.
		System.out.println("********************************************************************************"
				+ "\n*	total number of category \"Music\" records: " + musicCount
				+ "\n*	total number of category \"Education\" records: " + educationCount
				+ "\n*	total number of category \"Entertainment\" records: " + entertainmentCount
				+ "\n*	total number of category \"Science And Technology\" records: " + SandTCount
				+ "\n*	total number of category \"News and Politics\" records: " + NandPCount
				+ "\n*	total number of records reviewed: " + (recordCount + skipCount)
				+ "\n*	total number of output records: " + totalCount
				+ "\n*	total number of skipped records: " + skipCount
				+ "\n********************************************************************************");
	
		sparkContext.close();
	}
	
	private static Tuple2<String, String> skip() {
		skipCount++;
		return new Tuple2<String, String>(null, null);
	}
	
	public static class CSVLineParser {
		public static String[] splitLine2array(String line) {
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String[] results = new String[tokens.length];
			for(int i = 0; i < tokens.length; i++){
				results[i] = tokens[i].replace("\"", "");
			}
			return results;
		}
	}
}