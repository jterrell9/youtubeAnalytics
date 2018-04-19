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

public class DateRange2 {
	public static final String firtLine = "video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description";
	public static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static int skipCount = 0;
	public static String range;
	public static String rangeDate;
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//check correct usage
		Date lastWeekDate =  getDate(getTodayDate());
		lastWeekDate.setTime(new Date().getTime() - 604800000);
		rangeDate = dateFormat.format(lastWeekDate);
		System.out.println("*\tlast week's date: " + rangeDate);
		if(args.length != 2) {
			System.err.println("usage: $SPARK-HOME/bin/spark-submit --class DateRange ./target/<jar-file> <range> <hdfs path to \"USvideos.csv\">"
					+ "\nranges - lastWeek, lastMonth, lastSixMonths, lastYear, AllTime"
					+ "\nonly specify working path of file within the hdfs");
			System.exit(1);
		}
		//grab input directories
		range = args[0];
		String inputDir = args[1];
		
		Function<Tuple2<Tuple2<String, String>, String>, Boolean> rangeFilterFunc = null;
		if(range.equals("lastWeek")) {
			//last week filter
			rangeFilterFunc = new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
				@Override
				public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
					Date lastWeekDate = getDate(getTodayDate());
					Date recordDate = getDate(record._1()._1());
					if(lastWeekDate == null || recordDate == null) return false;
					lastWeekDate.setTime((new Date().getTime() - 604800000));
					return recordDate.after(lastWeekDate);
				}
			};
		}
		if(rangeFilterFunc == null) {
			System.err.println("*ERROR: no proper date range selected");
			System.exit(3);
		}
		
		//setup SparkConf and SparkContext
		SparkConf conf = new SparkConf().setAppName("videos within certain date range");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		//create JavaPairRDD K,V pair wih category_id as key and record as value
		JavaPairRDD<Tuple2<String, String>, String> rdd = sparkContext.textFile("hdfs://localhost:9000" + inputDir + "/USvideos.csv")
				.mapToPair(new PairFunction<String, Tuple2<String, String>, String>() {
			@Override
			public Tuple2<Tuple2<String, String>, String> call(String record) throws Exception {
				if(record.equals(firtLine)) {
					System.err.println("* reading file..."); 
					return skipTuple2(); 
				}
				String[] split = CSVLineParser.splitLine2array(record);
				if(split.length < 16) return skipTuple2();
				String publish_time = split[5];
				if(publish_time.length() < 10) return skipTuple2();
				publish_time = publish_time.substring(0, 10);
				String title = split[2];
				String video_id = split[0];
				String category_id = split[4];
				if(category_id == null || publish_time == null || publish_time.length()<10
						|| title == null || video_id == null
						|| video_id == null) {
					return skipTuple2();
				}
				String videoInfo = publish_time
						+ " - https://www.youtube.com/watch?v=" + video_id + " - title:" + title;
				if(category_id == null  || category_id.equals("") || publish_time == null || record == null) return skipTuple2();
				return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(publish_time, category_id), videoInfo);
			}	
		}).filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				if(record == null || record._1() == null || record._1()._1() == null || record._1()._2() == null
						|| record._2() == null) return false;
				return true;
			}
		}).filter(rangeFilterFunc).persist(StorageLevel.MEMORY_AND_DISK());
				
		JavaRDD<String> musicRDD = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("10");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> educationRDD = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("27");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> entertainmentRDD = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("24");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> scienceAndTechRDD = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("28");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		JavaRDD<String> newsAndPoliticsRDD = rdd.filter(new Function<Tuple2<Tuple2<String, String>, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, String> record) throws Exception {
				return record._1()._2().equals("25");
			}
		}).values().persist(StorageLevel.MEMORY_AND_DISK());
		
		Long musicCount = musicRDD.count();
		Long educationCount = educationRDD.count();
		Long entertainmentCount = entertainmentRDD.count();
		Long SandTCount = scienceAndTechRDD.count();
		Long NandPCount = newsAndPoliticsRDD.count();
		Long totalCount = musicCount + educationCount + entertainmentCount + SandTCount + NandPCount;
		
		//print out record counts.
		System.out.println("********************************************************************************"
				+ "\n*	total number of \"Music\" records in date range " + range + ": " + musicCount
				+ "\n*	total number of \"Education\" records in date range " + range + ": " + educationCount
				+ "\n*	total number of \"Entertainment\" records in date range " + range + ": " + entertainmentCount
				+ "\n*	total number of \"Science And Technology\" records in date range " + range + ": " + SandTCount
				+ "\n*	total number of \"News and Politics\" records in date range " + range + ": " + NandPCount
				+ "\n*	total number of records  in date range " + range + ": " + totalCount
				+ "\n********************************************************************************");
		
		//produce lists, and write to files
		List<String> musicListDateRange = musicRDD.collect();
		writeListToFile(musicListDateRange, "Music");
		List<String> educationListDateRange = educationRDD.collect();
		writeListToFile(educationListDateRange, "Education");
		List<String> entertainmemtListDateRange = entertainmentRDD.collect();
		writeListToFile(entertainmemtListDateRange, "Entertainment");
		List<String> sAndTListDateRange = scienceAndTechRDD.collect();
		writeListToFile(sAndTListDateRange, "Science And Technology");
		List<String> nAndPListDateRange = newsAndPoliticsRDD.collect();
		writeListToFile(nAndPListDateRange, "News And Politics");
		
		sparkContext.close();
	}
	
	@SuppressWarnings("deprecation")
	private static Date getDate(String date) {
		if(date.length() != 10 || date.charAt(0) != '2') {
			System.err.println("*ERROR: date: " + date);
			return null;
		}
		Integer year = Integer.parseInt(date.substring(0, 4));
		Integer month = Integer.parseInt(date.substring(5, 7));
		Integer day = Integer.parseInt(date.substring(8, 10));
		Date outputDate = new Date();
		outputDate.setYear(year);
		outputDate.setDate(day);
		outputDate.setMonth(month);
		return outputDate;
	}
	
	private static String getTodayDate() {
		return dateFormat.format(new Date());
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
		}else if(year1 == year2) {
			if(month1 > month2) {
				return true;
			}else if(month1 < month2) {
				return false;
			}else if(month1 == month2) {
				if(day1 > day2) {
					return true;
				}else if(day1 < day2) {
					return false;
				}else if(day1 == day2) {
					return true;
				}
			}
		}
		return false;
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
	
	private static Tuple2<Tuple2<String, String>, String> skipTuple2() {
		skipCount++;
		return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(null, null), null);
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