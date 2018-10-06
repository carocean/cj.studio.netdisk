package cj.lns.chip.sos.disk;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import cj.lns.chip.sos.cube.framework.MyJsonWriter;
import cj.studio.ecm.EcmException;
import cj.ultimate.gson2.com.google.gson.Gson;

/*
 * 格式：
 * {'name':'%s','parent':'%s','dbname':'%s','diskinfo':{}}
 * 
 * 其中仅当parent=-1时才有diskinfo，即仅当是网盘时才有
 * 当parent=-1时dbname存放的是shared立方体的库名
 */
public class NameContainer implements INamedContainer {
	MongoCollection<Document> col;

	public NameContainer(MongoClient client) {
		MongoDatabase db = client.getDatabase("lns_namespace");
		col = db.getCollection("named_container");
		if (col == null) {
			db.createCollection("named_container");
			col = db.getCollection("named_container");
		}
	}

	@Override
	public long countCube(String diskname) {
		String wi=String.format("{'$or':[{'parent':'%s'},{'name':'%s','parent':'-1'}]}", diskname,diskname);
		Document filter=Document.parse(wi);
		return col.count(filter);
	}

	@Override
	public List<String> enumCubePhyName(String diskname) {
		String wi=String.format("{'$or':[{'parent':'%s'},{'name':'%s','parent':'-1'}]}", diskname,diskname);
		Document filter=Document.parse(wi);
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("dbname", 1));
		List<String> list = new ArrayList<>();
		for (Document doc : find) {
			list.add(doc.getString("dbname"));
		}
		return list;
	}

	@Override
	public List<String> enumCubeName(String diskname) {
		BasicDBObject filter = new BasicDBObject("parent", diskname);
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("name", 1));
		List<String> list = new ArrayList<>();
		for (Document doc : find) {
			list.add(doc.getString("name"));
		}
		return list;
	}

	@Override
	public List<String> enumDiskName() {
		BasicDBObject filter = new BasicDBObject("parent", "-1");
		FindIterable<Document> find = col.find(filter);
		List<String> list = new ArrayList<>();
		for (Document doc : find) {
			list.add(doc.getString("name"));
		}
		return list;
	}

	@Override
	public boolean existsDiskName(String diskname, String cubeName) {
		Document filter = Document.parse(String
				.format("{'name':'%s','parent':'%s'}", cubeName, diskname));
		return col.count(filter) > 0;
	}

	@Override
	public boolean existsDiskName(String diskname) {
		Document filter = Document
				.parse(String.format("{'name':'%s','parent':'-1'}", diskname));
		if (col.count(filter) > 0) {
			return true;
		}
		return false;
	}

	@Override
	public void removeDisk(String diskname) {
		BasicDBObject filter = new BasicDBObject("parent",diskname);
		col.deleteMany(filter);
		
		Document filter2 = Document.parse(String
				.format("{'name':'%s','parent':'-1'}", diskname));
		col.deleteOne(filter2);
		
	}
	@Override
	public void removeCubeName(MongoClient client, String diskname,
			String cubeName) {
		Document filter=Document.parse(String.format("{'name':'%s','parent':'%s'}", cubeName,diskname));
		col.deleteOne(filter);
	}
	@Override
	public String diskCubePhyName(String diskname, String cubeName) {
		Document filter = Document.parse(String
				.format("{'name':'%s','parent':'%s'}", cubeName, diskname));
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("dbname", 1)).limit(1);
		Document doc = find.first();
		if (doc == null) {
			throw new EcmException(
					String.format("不存在网盘%s或不存在立方体%s", diskname, cubeName));
		}
		return doc.getString("dbname");
	}
	@Override
	public String diskCubeName(String diskname, String cubePhyName) {
		Document filter = Document.parse(String
				.format("{'dbname':'%s','parent':'%s'}", cubePhyName, diskname));
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("name", 1)).limit(1);
		Document doc = find.first();
		if (doc == null) {
			throw new EcmException(
					String.format("不存在网盘%s或不存在立方体,cube物理名：%s", diskname, cubePhyName));
		}
		return doc.getString("name");
	}
	@Override
	public String diskSharedCubePhyName(String diskname) {
		Document filter = Document
				.parse(String.format("{'name':'%s','parent':'-1'}", diskname));
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("dbname", 1)).limit(1);
		Document doc = find.first();
		if (doc == null) {
			throw new EcmException(String.format("不存在网盘：%s", diskname));
		}
		return doc.getString("dbname");
	}

	@Override
	// 系统cube用于维护网盘库名关系的立方体，它为整个MONGODB所共有。
	// 采用父子结构的名与标识映射的方式
	public String appendDiskName(MongoClient client, String diskname,
			DiskInfo info) {
		Document filter = Document
				.parse(String.format("{'name':'%s','parent':'-1'}", diskname));
		if (col.count(filter) > 0) {
			throw new EcmException(String.format("已存在网盘：%s", diskname));
		}
		// name代表网盘名或网盘内的立方体名，parent是立方体的归属名，如果
		// 是网盘则为-1，非-1则是网盘内的立方体，dbname是mongodb的集合名也就是立方体库
		String json = new Gson().toJson(info);
		String cubedbName = UUID.randomUUID().toString().replace("-", "");
		// 当parent=-1时dbname存放的是shared立方体的库名
		Document document = Document.parse(String.format(
				"{'name':'%s','parent':'-1','dbname':'%s','diskinfo':%s}",
				diskname, cubedbName, json));
		col.insertOne(document);
		return cubedbName;
	}

	@Override
	public String appendDiskName(MongoClient client, String diskname,
			String cubeName) {
		if (!existsDiskName(diskname)) {
			throw new EcmException(String.format("不存在网盘：%s", diskname));
		}
		Document filter = Document.parse(String
				.format("{'name':'%s','parent':'%s'}", cubeName, diskname));
		if (col.count(filter) > 0) {
			throw new EcmException(
					String.format("网盘：%s中忆存在立方体：%s", diskname, cubeName));
		}
		String cubedbName = UUID.randomUUID().toString().replace("-", "");
		// name代表网盘名或网盘内的立方体名，parent是立方体的归属名，如果
		// 是网盘则为-1，非-1则是网盘内的立方体，dbname是mongodb的集合名也就是立方体库
		Document document = Document.parse(
				String.format("{'name':'%s','parent':'%s','dbname':'%s'}",
						cubeName, diskname, cubedbName));
		col.insertOne(document);
		return cubedbName;
	}

	@Override
	public DiskInfo diskInfo(String diskname) {
		Document filter = Document
				.parse(String.format("{'name':'%s','parent':'-1'}", diskname));
		FindIterable<Document> find = col.find(filter)
				.projection(new BasicDBObject("diskinfo", 1)).limit(1);
		Document doc = find.first();
		if (doc == null) {
			throw new EcmException(String.format("不存在网盘：%s", diskname));
		}
		MyJsonWriter writer = new MyJsonWriter(new StringWriter(),
				new JsonWriterSettings());
		new DocumentCodec().encode(writer, (Document) doc.get("diskinfo"),
				EncoderContext.builder().isEncodingCollectibleDocument(true)
						.build());
		DiskInfo info = new Gson().fromJson(writer.getWriter().toString(),
				DiskInfo.class);
		return info;
	}

	@Override
	public void updateDiskInfo(String diskname, DiskInfo info) {
		if (!existsDiskName(diskname)) {
			throw new EcmException(String.format("不存在网盘：%s", diskname));
		}
		Document filter = Document
				.parse(String.format("{'name':'%s','parent':'-1'}", diskname));
		String json = new Gson().toJson(info);
		Document doc = Document.parse(json);
		col.updateOne(filter, new BasicDBObject("$set", doc));
	}
}
