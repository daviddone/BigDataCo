package com.david.es;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.60.0.8"), 9300));
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("slave1"), 9300))
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}
	
	public static void get(){
		GetResponse response = getClient().prepareGet("twitter", "tweet", "1").get();
		System.out.println(response.getSourceAsString());
	}
}
