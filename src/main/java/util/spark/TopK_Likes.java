package main.java.util.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

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
//30	Movies	27	Education
//42	Shorts	24	Entertainment
//28	Science & Technology
//25	News & Politics

public class TopK_Likes {
	public static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	public static int skipCount = 0;
	public static Integer k = null;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//check correct usage
		if(args.length != 2) {
			System.err.println("usage: $SPARK-HOME/bin/spark-submit --class TopK_Likes ./target/<jar-file> <k> <input-dir>"
					+ "\nk --> how many records you want to return"
					+ "\nuse whole numbers for k"
					+ "\nonly specify working path of file within the hdfs");
			System.exit(1);
		}
		//grab input directories
		k = Integer.parseInt(args[0]);
		String inputDir = args[1];
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("Filter by category_id, sort by views");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		//create input RDD
		JavaRDD<String> inputRDD = sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/USvideos.csv");
		//create JavaPairRDD K,V pair wih category_id as key and record as value
		JavaPairRDD<String, String> categoryRDD = inputRDD.mapToPair(new PairFunction<String, String, String>() {
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
		
		PairFunction<String, String, String> mapLikesFunc = new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String record) {
				if(record.equals(firtLine)) {
					System.err.print("*	mapping category music..."); 
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
						+ "\tviews: " + views + "\tlikes: " + likes
						+ "\tcategory: " + category_id;
				return new Tuple2<String, String>(likes, video);
			}
		};
		
		//skip record filter Function
		Function<Tuple2<String, String>, Boolean> skipFilterFunc = new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> skipValue) throws Exception {
				if(skipValue == null) return false;
				return true;
			}
		};
		
		//create RDD chain
		JavaRDD<String> sortedMusicRDD = musicRDD.mapToPair(mapLikesFunc).filter(skipFilterFunc).sortByKey(false).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> sortedEducationRDD = educationRDD.mapToPair(mapLikesFunc).filter(skipFilterFunc).sortByKey(false).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> sortedEntertainmentRDD = entertainmentRDD.mapToPair(mapLikesFunc).filter(skipFilterFunc).sortByKey(false).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> sortedSandTRDD = scienceAndTechRDD.mapToPair(mapLikesFunc).filter(skipFilterFunc).sortByKey(false).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> sortedNandPRDD = newsAndPoliticsRDD.mapToPair(mapLikesFunc).filter(skipFilterFunc).sortByKey(false).values().persist(StorageLevel.MEMORY_AND_DISK());
		
		Long musicCount = sortedMusicRDD.count();
		Long educationCount = sortedEducationRDD.count();
		Long entertainmentCount = sortedEntertainmentRDD.count();
		Long SandTCount = sortedSandTRDD.count();
		Long NandPCount = sortedNandPRDD.count();
		Long totalCount = musicCount + educationCount + entertainmentCount + SandTCount + NandPCount;
		
		//print out record counts.
		System.out.println("********************************************************************************"
				+ "\n*	total number of category \"Music\" records: " + musicCount
				+ "\n*	total number of category \"Education\" records: " + educationCount
				+ "\n*	total number of category \"Entertainment\" records: " + entertainmentCount
				+ "\n*	total number of category \"Science And Technology\" records: " + SandTCount
				+ "\n*	total number of category \"News and Politics\" records: " + NandPCount
				+ "\n*	total number of records reviewed: " + totalCount
				+ "\n*	total number of records: " + (totalCount + skipCount)
				+ "\n*	total number of skipped records: " + skipCount
				+ "\n********************************************************************************");
		
		//produce top k lists, and write to files
		List<String> musicListTopK = sortedMusicRDD.take(k);
		writeListToFile(musicListTopK, "Music");
		List<String> educationListTopK = sortedEducationRDD.take(k);
		writeListToFile(educationListTopK, "Education");
		List<String> entertainmemtListTopK = sortedEntertainmentRDD.take(k);
		writeListToFile(entertainmemtListTopK, "Entertainment");
		List<String> sAndTListTopK = sortedSandTRDD.take(k);
		writeListToFile(sAndTListTopK, "Science And Technology");
		List<String> nAndPListTopK = sortedNandPRDD.take(k);
		writeListToFile(nAndPListTopK, "News And Politics");
		
		sparkContext.close();
	}
	
	private static void writeListToFile(List<String> list, String category) {
		int recordOrder = 0;
		FileWriter output;
		try {
			output = new FileWriter(new File("./top" + k + "_" + category+ "_Likes.txt"));
			for(String record : list) {
				recordOrder++;
				output.write(recordOrder + ": " + record + "\n");
			}
			output.close();
		}catch(IOException ioe) {
			System.err.println("ERROR: file not found");
			System.exit(2);
		}
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