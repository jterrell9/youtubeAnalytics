package main.java.catgoryJson;

public class VideoCategoryListResponse {
	private String kind;
	private String etag;
	private VideoCategory[] items;
	
	public VideoCategory getItem(int category_id) {
		for(int i=0; i<size(); i++) {
			if(items[i].getCategory_id() == category_id) return items[i];
		}
		throw new RuntimeException("the category_id " + category_id + " does not exist");
	}
	
	public int[] getCategory_ids() {
		int[] category_ids = new int[size()];
		for(int i=0; i<size(); i++) {
			category_ids[i] = items[i].getCategory_id();
		}
		return category_ids;
	}
	
	public int size() {
		return items.length;
	}
	
	@Override
	public String toString() {
		String str = "";
		for(int i=0; i<items.length; i++) {
			str += items[i] + "\n";
		}
		return str;
	}
}