package cj.lns.chip.sos.disk.util;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import cj.studio.ecm.IServiceSite;
import cj.studio.ecm.context.ElementGet;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.gson2.com.google.gson.JsonElement;
import cj.ultimate.gson2.com.google.gson.JsonObject;
import cj.ultimate.util.StringUtil;

public class MongoClientService {
	MongoClient client;

	/**
	 * Assembly.properties中配置：
	 * sos.mongo.address=[{'host':'192.168.201.210','port':-1}]
	 * <pre>
	 *
	 * </pre>
	 * @param site
	 */
	public void init(IServiceSite site) {
		String address = site.getProperty("sos.mongo.address");
		List<ServerAddress> seeds = new ArrayList<>();
		JsonElement je = new Gson().fromJson(address, JsonElement.class);
		for (JsonElement j : je.getAsJsonArray()) {
			JsonObject jo = j.getAsJsonObject();
			String host = ElementGet.getJsonProp(jo.get("host"));
			String portStr = ElementGet.getJsonProp(jo.get("port"));
			int port = 0;
			if (!StringUtil.isEmpty(portStr)) {
				port = Integer.valueOf(portStr);
			}
			if (port < 1) {
				seeds.add(new ServerAddress(host));
			} else {
				seeds.add(new ServerAddress(host, port));
			}
		}
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		client = new MongoClient(seeds, credential, options);
	}
	public void init(List<ServerAddress> seeds,List<MongoCredential> credential,MongoClientOptions options) {
		client = new MongoClient(seeds, credential, options);
	}
	public MongoClient client() {

		return client;
	}
}