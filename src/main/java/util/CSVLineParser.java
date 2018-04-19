package main.java.util;

import java.util.HashMap;
import java.util.Map;

public class CSVLineParser {
	
	public static final String[] keys = {
			"video_id",
			"trending_date",
			"title",
			"channel_title",
			"category_id",
			"publish_time",
			"tags",
			"views",
			"likes",
			"dislikes",
			"comment_count",
			"thumbnail_link",
			"comments_disabled",
			"ratings_disabled",
			"video_error_or_removed",
			"description"
			};
	
	public static String[] splitLine2array(String line) {
		String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		String[] results = new String[tokens.length];
		for(int i = 0; i < tokens.length; i++){
			results[i] = tokens[i].replace("\"", "");
		}
		return results;
	}
	public static Map<String, String> splitLine2map(String line) {
		Map<String, String> map = new HashMap<String, String>();
		String[] tmpArry;
		for(int k=0; k<keys.length; k++) {
			int delimIndex = 0;
			if(line.charAt(delimIndex) == '\"') {
				delimIndex++;
				while(line.charAt(delimIndex) != '\"') {
					delimIndex++;
				}
				map.put(keys[k], line.substring(1, delimIndex));
				delimIndex += 2;
				line = line.substring(delimIndex);
				continue;
			}else {
				tmpArry = splitLine2array(line);
				while(k < keys.length) {
					if(tmpArry[0].equals("")) {
						tmpArry[0] = null;
						map.put(keys[k], tmpArry[0]);
						if(k >= keys.length - 1) {
							break;
						}
						line = line.substring(1);
						break;
					}else {
						map.put(keys[k], tmpArry[0]);
						if(k >= keys.length - 1) {
							break;
						}
						line = line.substring(tmpArry[0].length() + 1);
						break;
					}
				}
			}
		}
		return map;
	}
}