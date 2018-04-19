package main.java.catgoryJson;

public class VideoCategory {
	private String kind;
	private String etag;
	private String id;
	private Snippet snippet;
	
	public int getCategory_id() {
		return Integer.parseInt(id);
	}
	
	public String getTitle() {
		return snippet.toString();
	}
	
	@Override
	public String toString() {
		return id + "\t" + snippet.toString();
	}
}