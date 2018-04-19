package main.java.util.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
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

//The following application filters youtube video data based on how recent the video post was made
//filters include: last week, last month, last year, all time
public class DateRange {
	private static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	private static final String usagePrompt = "usage: $SPARK-HOME/bin/spark-submit --class DateRange ./target/<jar-file> <range> <hdfs path to \"USvideos.csv\">"
			+ "\nranges: last-week, last-month, last-6-Months, last-year, no-date-filter"
			+ "\nonly specify working path of file within the hdfs";
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static final String LIST_ALL_CATEGORIES = "LISTALL";
	public static String range;
	public static String inputDir;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//check correct usage
		if(args.length != 2) {
			System.err.println(usagePrompt);
			System.exit(1);
		}
		//grab input directories
		range = args[0];
		inputDir = args[1];
		//date range filter function
		Function<Tuple2<Tuple2<String, String>, String>, Boolean> rangeFilterFunc = 
				new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				Date rangeDate = new Date();
				String rangeDateStr = dateFormat.format(rangeDate);
				Integer year = Integer.parseInt(rangeDateStr.substring(0, 4));
				Integer month = Integer.parseInt(rangeDateStr.substring(5, 7));
				Integer day = Integer.parseInt(rangeDateStr.substring(8, 10));
				switch(range) {
					case "last-week": 
						rangeDate.setTime((rangeDate.getTime() - 604800000));
						rangeDateStr = dateFormat.format(rangeDate);
						break;
					case "last-month":
						if(month == 1) {
							year--;
							month = 12;
						}else {
							month--;
						}
						break;
					case "last-6-Months":
						if(month < 7) {
							month = 12 - (month % 6);
						}else {
							month -= 6;
						}
						break;
					case "last-year":
						year--;
						break;
					case "no-date-filter":
						return true;
					default:
						System.err.println(usagePrompt);
						System.exit(2);
					}
				if(month < 10) {
					if(day < 10) rangeDateStr = year + "-0" + month + "-0" + day;
					else rangeDateStr = year + "-0" + month + "-" + day;
				}else {
					if(day < 10) rangeDateStr = year + "-" + month + "-0" + day;
					else rangeDateStr = year + "-" + month + "-" + day;
				}
				return isAfter(record._1()._1(), rangeDateStr);
			}
		};
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("videos within certain date range");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		//create JavaPairRDD K,V pair wih category_id as key and record as value
		JavaPairRDD<Tuple2<String, String>, String> rdd = 
				sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/USvideos.csv")
				.mapToPair(new PairFunction<String, Tuple2<String, String>, String>() {
			@Override
			public Tuple2<Tuple2<String, String>, String> call(String record) throws Exception {
				if(record.equals(firtLine)) {
					System.out.println("*PROGRAM OUTPUT >> reading file \"USvideos.csv\" into Spark RDD..."); 
					return null;
				}
				//video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length != 16) return null;
				String publish_time = split[5];
				if(publish_time.length() < 10) return null;
				publish_time = publish_time.substring(0, 10);
				if(publish_time.length() != 10 || publish_time.charAt(0) != '2' || publish_time.equals("")) return null;
				Date publishDate = dateFormat.parse(publish_time);
				String title = split[2];
				String video_id = split[0];
				String category_id = split[4];
				String videoInfo = publish_time + " - https://www.youtube.com/watch?v=" + video_id + " - title:" + title;
				return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(publish_time, category_id), videoInfo);
			}
		}).filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				if(record == null) return false;
				return true;
			}
		}).filter(rangeFilterFunc).persist(StorageLevel.MEMORY_AND_DISK());
		//mapper function to set date as key for sorting
		PairFunction<Tuple2<Tuple2<String, String>, String>, String, String> mapDateToKeyMapper = new PairFunction<Tuple2<Tuple2<String, String>, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return new Tuple2<String, String>(record._1()._1(), record._2());
			}
		};
		//create 
		List<String> musicList = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("10");
			}
		}).mapToPair(mapDateToKeyMapper).sortByKey(true).values().collect();
		writeListToFile(musicList, "Music");
		
		List<String> educationList = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("27");
			}
		}).mapToPair(mapDateToKeyMapper).sortByKey(true).values().collect();
		writeListToFile(educationList, "Education");
		
		List<String> entertainmentList = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("24");
			}
		}).mapToPair(mapDateToKeyMapper).sortByKey(true).values().collect();
		writeListToFile(entertainmentList, "Entertainment");
		
		List<String> scienceAndTechList = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("28");
			}
		}).mapToPair(mapDateToKeyMapper).sortByKey(true).values().collect();
		writeListToFile(scienceAndTechList, "Science and Technology");
		
		
		List<String> newsAndPoliticsList = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("25");
			}
		}).mapToPair(mapDateToKeyMapper).sortByKey(true).values().collect();
		writeListToFile(newsAndPoliticsList, "News And Politics");
		
		long musicCount = musicList.size();
		long educationCount = educationList.size();
		long entertainmentCount = entertainmentList.size();
		long scienceAndTechListCount = scienceAndTechList.size();
		long newsAndPoliticsListCount = newsAndPoliticsList.size();
		long totalCount = musicCount + educationCount + entertainmentCount + scienceAndTechListCount + newsAndPoliticsListCount;
		
		//print out record counts.
		System.out.println("********************************************************************************"
				+ "\n*	total number of \"Music\" records in date range " + range + ": " + musicCount
				+ "\n*	total number of \"Education\" records in date range " + range + ": " + educationCount
				+ "\n*	total number of \"Entertainment\" records in date range " + range + ": " + entertainmentCount
				+ "\n*	total number of \"Science And Technology\" records in date range " + range + ": " + scienceAndTechListCount
				+ "\n*	total number of \"News and Politics\" records in date range " + range + ": " + newsAndPoliticsListCount
				+ "\n*	total number of records  in date range " + range + ": " + totalCount
				+ "\n********************************************************************************");
		
		sparkContext.close();
	}
	
	private static boolean isAfter(String date1, String date2) {
		if(date1.length() != 10 || date1.charAt(0) != '2') {
			System.err.println("*ERROR: date1: " + date1);
			return false;
		}
		if(date2.length() != 10 || date2.charAt(0) != '2') {
			System.err.println("*ERROR: date2: " + date2);
			return false;
		}
		Integer year1 = Integer.parseInt(date1.substring(0, 4));
		Integer month1 = Integer.parseInt(date1.substring(5, 7));
		Integer day1 = Integer.parseInt(date1.substring(8, 10));
		Integer year2 = Integer.parseInt(date2.substring(0, 4));
		Integer month2 = Integer.parseInt(date2.substring(5, 7));
		Integer day2 = Integer.parseInt(date2.substring(8, 10));
		if(year1 > year2) {
			return true;
		}else if(year1 < year2) {
			return false;
		}else {
			if(month1 > month2) {
				return true;
			}else if(month1 < month2) {
				return false;
			}else {
				if(day1 > day2) {
					return true;
				}else if(day1 < day2) {
					return false;
				}else {
					return true;
				}
			}
		}
	}
	
	private static void writeListToFile(List<String> list, String category) {
		int recordOrder = 0;
		FileWriter output;
		try {
			output = new FileWriter(new File("./" + category + "_" + range + ".txt"));
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
