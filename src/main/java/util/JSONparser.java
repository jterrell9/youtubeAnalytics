package main.java.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import main.java.catgoryJson.VideoCategoryListResponse;

public class JSONparser {
	public static final String LIST_ALL = "LISTALL_CATEGORY";
	
	public static String getCategory(String category_id) {
		//create GsonBuilder for Gson object
		Gson categoriesGson = new GsonBuilder().create();
		//read json file
		FileReader jsonReader = null;
		try {
			jsonReader = new FileReader("./US_category_id.json");
		}catch (IOException ioe) {
			System.err.println("json file not found");
			System.exit(2);
		}
		//deserialize json
		VideoCategoryListResponse categories = categoriesGson.fromJson(jsonReader, VideoCategoryListResponse.class);
		try {
			jsonReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Map<Integer, String> categoryMap = mapCategories(categories);
		if(category_id.equals(LIST_ALL)) {
			return categories.toString();
		}
		Integer category_idInt = null;
		try {
			category_idInt = Integer.parseInt(category_id);
		}catch(NumberFormatException nfe) {
			System.err.println("*ERROR >> category_id not number");
			System.exit(3);
		}
		String category = categoryMap.get(category_idInt);
		return category;
	}
	public static Map<Integer, String> mapCategories(VideoCategoryListResponse categories) {
		Map<Integer, String> categoryMap = new HashMap<Integer, String>();
		int[] category_ids = categories.getCategory_ids();
		for(int i=0; i<categories.size(); i++) {
			categoryMap.put(category_ids[i], categories.getItem(category_ids[i]).getTitle());
		}
		return categoryMap;
	}
}
