package main.java.util;

import java.util.HashMap;
import java.util.Map;

import main.java.catgoryJson.VideoCategoryListResponse;

public class CategoryMapper {
	public static Map<Integer, String> mapCategories(VideoCategoryListResponse categories) {
		Map<Integer, String> categoryMap = new HashMap<Integer, String>();
		int[] category_ids = categories.getCategory_ids();
		for(int i=0; i<categories.size(); i++) {
			categoryMap.put(category_ids[i], categories.getItem(category_ids[i]).getTitle());
		}
		return categoryMap;
	}
}
