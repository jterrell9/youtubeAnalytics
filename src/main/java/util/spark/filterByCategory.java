package main.java.util.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//$SPARK-HOME/bin/spark-submit --class Question4 ./target <jar-file> <post-data-dir> <user-data-dir> <output-dir>
//[0]	"video_id",
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
//[15 ] "description"

public class filterByCategory {
	public static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	public static int skipCount = 0;
	public static String inputCategory_id = null;
	
	public static void main(String[] args) {
		//check correct usage
		if(args.length != 3) {
			System.err.println("usage: $SPARK-HOME/bin/spark-submit --class filterByCategory ./target/<jar-file> <category_id> <input-dir> <output-dir>");
			return;
		}
		//grab input directories
		inputCategory_id = args[0];
		String inputDir = args[1];
		String outputDir = args[2];
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("Filter by category_id");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		//skip record filter Function
		Function<Tuple2<String, String>, Boolean> skipFilter = new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> skipValue) throws Exception {
				if(skipValue == null) return false;
				return true;
			}
		};
		//Category Filter Function
		Function<Tuple2<String, String>, Boolean> categoryFilter = new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> record) {
				if(record == null) return false;
				if(doesCategoryMatch(record._1())) return true;
				else return false;
			}
		};
		//create RDD chain
		JavaRDD<String> rdd = sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/*")
				.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String record) {
				if(record.equals(firtLine)) {
					System.err.print("first line - "); 
					return skip(); 
				}
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length < 16) return skip();
				String category_id = split[4];
				String publish_time = split[5];
				String title = split[2];
				String video_id = split[0];
				String views = split[7];
				String likes = split[8];
				if(category_id == null || publish_time == null || publish_time.length()<10
						|| title == null || video_id == null
						|| video_id == null || views == null || likes == null) {
					return skip();
				}
				String video = "title: " + title + " (" + video_id + ")"
						+ "\tdate: " + publish_time.substring(5, 7) + "/" + publish_time.substring(8, 10) + "/" + publish_time.substring(0, 4)
						+ "\tviews: " + views + "\tlikes: " + likes;
				return new Tuple2<String, String>(category_id, video);
			}
		}).filter(skipFilter).filter(categoryFilter).values();
		
		Long recordCount = rdd.count();
		System.out.println("total number of category " + inputCategory_id + " records: " + recordCount);
			
		rdd.saveAsTextFile("hdfs://localhost:9000" + outputDir);
		sparkContext.close();
	}
	
	private static boolean doesCategoryMatch(String category_id) {
		return inputCategory_id.equals(category_id);
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