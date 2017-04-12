package com.lxing.domain;

public class Article {
	private String id; //文章ID 可作为主键使用
	private String title;// 标题
	private String author;// 作者
	private String date;// 日期
	private String content;// 文章内容
	private String url;// 目标url地址

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	@Override
	public String toString() {
		return "Article [id="+id+", title=" + title + ", author=" + author + ", date=" + date + ", url=" + url + ", content="
				+ content + "]";
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
