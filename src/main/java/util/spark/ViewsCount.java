package main.java.util.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

public class ViewsCount {
	public static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	public static int skipCount = 0;
	public static UnsignedLong k = null;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//check correct usage
		if(args.length != 2) {
			System.err.println("usage: $SPARK-HOME/bin/spark-submit --class ViewsCount ./target/<jar-file> <input-dir> <output-dir>"
					+ "\nonly specify working path (the directory) of file within the hdfs");
			System.exit(1);
		}
		//grab input directories
		String inputDir = args[0];
		String outputDir = args[1];
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("Filter by category_id, sort by views");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaPairRDD<String, UnsignedLong> rdd = sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/USvideos.csv")
				.mapToPair(new PairFunction<String, String, UnsignedLong>() {
			@Override
			public Tuple2<String, UnsignedLong> call(String record) {
				if(record.equals(firtLine)) {
					System.err.print("***	mapping categories..."); 
					return skip(); 
				}
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length < 16) return skip();
				String category_id = split[4];
				if(category_id == null) return skip(); 
				String category;
				switch(category_id) {
					case "10": category = "Music"; break;
					case "27": category = "Education"; break;
					case "24": category = "Entertainment"; break;
					case "28": category = "Science and Technology"; break;
					case "25": category = "News and Politics"; break;
					default: return skip();
				}
				String views = split[7];
				if(views == null) {
					return skip();
				}
				UnsignedLong viewCount = UnsignedLong.valueOf(views);
				return new Tuple2<String, UnsignedLong>(category, viewCount);
			}
		}).filter(new Function<Tuple2<String, UnsignedLong>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, UnsignedLong> skipValue) throws Exception {
				if(skipValue == null || skipValue._1() == null || skipValue._2() == null) return false;
				return true;
			}
		}).reduceByKey(new Function2<UnsignedLong, UnsignedLong, UnsignedLong>() {
			@SuppressWarnings("deprecation")
			@Override
			public UnsignedLong call(UnsignedLong recordViews1, UnsignedLong recordViews2) throws Exception {
				if(recordViews1.longValue() < 0) System.out.println("Views sum: " + recordViews1 + " + record views: " + recordViews2 
						+ " = " + (recordViews1.add(recordViews2)));
				return recordViews1.add(recordViews2);
			}
		});
		
		rdd.saveAsTextFile("hdfs://localhost:9000" + outputDir);
		
		Long recordCount = rdd.count();
		
		//print out record counts.
		System.out.println("********************************************************************************"
				+ "\n*	total number of records reviewed: " + (recordCount + skipCount)
				+ "\n*	total number of output records: " + recordCount
				+ "\n*	total number of skipped records: " + skipCount
				+ "\n********************************************************************************");
		
		sparkContext.close();
	}
	
	private static Tuple2<String, UnsignedLong> skip() {
		skipCount++;
		return new Tuple2<String, UnsignedLong>(null, null);
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