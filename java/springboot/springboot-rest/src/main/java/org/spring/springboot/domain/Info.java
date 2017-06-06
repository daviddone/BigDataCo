package org.spring.springboot.domain;


public class Info {
	private String title;
	private String content;
	private String desc;
	private String dateTime;
	private String detail_href;
	private String field;
	private String field_site;
	
	
	public Info(String title, String desc, String detail_href) {
		super();
		this.title = title;
		this.desc = desc;
		this.detail_href = detail_href;
	}
	
	public Info(String title, String content, String desc, String dateTime,
			String detail_href, String field, String field_site) {
		super();
		this.title = title;
		this.content = content;
		this.desc = desc;
		this.dateTime = dateTime;
		this.detail_href = detail_href;
		this.field = field;
		this.field_site = field_site;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	public String getDetail_href() {
		return detail_href;
	}
	public void setDetail_href(String detail_href) {
		this.detail_href = detail_href;
	}
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getField_site() {
		return field_site;
	}
	public void setField_site(String field_site) {
		this.field_site = field_site;
	}
	
	
	
}
