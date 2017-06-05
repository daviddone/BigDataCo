package com.david.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;

import com.david.es.bean.Info;

public class esUtil {
	
	public static Client client = null;
	
	public static  Client getClient() {
		if(client!=null){
			return client;
		}
		
		Settings settings = Settings.settingsBuilder().put("cluster.name", "my-application")
							.put("client.transport.sniff", true).build(); //开启集群嗅探功能，允许其动态添加新主机并删除旧主机
		try {
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.60.0.8"), 9300));//ip
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("slave1"), 9300)) //hostname
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}
	
	public static void get(){
		GetResponse response = getClient().prepareGet("twitter", "tweet", "1").get();
		System.out.println(response.getSourceAsString());
	}
	public static void addIndexByJson(String json){
		IndexResponse response = getClient().prepareIndex("twitter", "tweet")
		        .setSource(json)
		        .get();
	}
	
	public static String addIndexByObject(String index,String type,Info info){
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("title", info.getTitle());
		hashMap.put("desc", info.getDesc());
		hashMap.put("detail_href", info.getDetail_href());
		
		IndexResponse response = getClient().prepareIndex(index, type).setSource(hashMap).execute().actionGet();
		//response.getIndex()  response.getType() response.getVersion() response.isCreated()
		return response.getId();
	}
	
	public static Map<String, Object> search(String key,String index,String type,int start,int row){
		SearchRequestBuilder builder = getClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setFrom(start);
		builder.setSize(row);
		//设置高亮字段名称
		builder.addHighlightedField("title");
		builder.addHighlightedField("desc");
		//设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		//设置高亮后缀
		builder.setHighlighterPostTags("</font>");
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if(!"".equals(key)){
			builder.setQuery(QueryBuilders.multiMatchQuery(key, "title","desc"));
		}
		builder.setExplain(true);//TODO 中文模糊查询。
		SearchResponse searchResponse = builder.get();
		
		SearchHits hits = searchResponse.getHits();
		long total = hits.getTotalHits();
		Map<String, Object> map = new HashMap<String,Object>();
		SearchHit[] hits2 = hits.getHits();
		map.put("count", total);
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (SearchHit searchHit : hits2) {
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Map<String, Object> source = searchHit.getSource();
			if(highlightField!=null){
				Text[] fragments = highlightField.fragments();
				String name = "";
				for (Text text : fragments) {
					name+=text;
				}
				source.put("title", name);
			}
			HighlightField highlightField2 = highlightFields.get("desc");
			if(highlightField2!=null){
				Text[] fragments = highlightField2.fragments();
				String desc = "";
				for (Text text : fragments) {
					desc+=text;
				}
				source.put("desc", desc);
			}
			list.add(source);
		}
		map.put("dataList", list);
		return map;
	}
}
