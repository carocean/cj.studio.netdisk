package cj.lns.chip.sos.cube.framework;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import cj.lns.chip.sos.disk.INamedContainer;
import cj.lns.chip.sos.disk.NameContainer;
import cj.studio.ecm.EcmException;

class Transcation implements ITranscation {
	protected static final String LNS_SYSTEM = "lns_system";
	protected MongoClient client;
	protected String id;
	private boolean containsPenddingRows;
	protected MongoCollection<Document> process;
	protected MongoCollection<Document> jobs;
	protected static INamedContainer container;

	Transcation(Cube cube) {
		if(client==null)
			this.client = cube.client;
		if (!exists(client, LNS_SYSTEM)) {
			CubeConfig conf = new CubeConfig();
			create(client, LNS_SYSTEM, conf);
		}
		if (container == null) {
			container = new NameContainer(client);
		}
		MongoDatabase cubedb = client.getDatabase(LNS_SYSTEM);
		process = cubedb.getCollection("sys.trans.process");
		jobs = cubedb.getCollection("sys.trans.jobs");
	}

	@Override
	public String id() {
		return id;
	}
	
	void init() {
		Map<String, Object> row = new HashMap<>();
		row.put("state", "init");
		row.put("ctime", System.currentTimeMillis());
		Document document = new Document(row);
		process.insertOne(document);
		ObjectId oid = document.getObjectId("_id");
		if (oid != null) {
			id = oid.toHexString();
		}
	}
	
	public static boolean exists(MongoClient client, String name) {
		MongoIterable<String> it = client.listDatabaseNames();
		for (String n : it) {
			if (n.equals(name)) {
				return true;
			}
		}
		return false;
	}
	
	public static ICube create(MongoClient client, String name, CubeConfig conf) {
		return create(client, name, conf, Transcation.class.getClassLoader());
	}
	public static ICube create(MongoClient client, String name, CubeConfig conf,ClassLoader cl) {
		Cube cube = new Cube(cl);
		MongoDatabase cubedb = client.getDatabase(name);
		cube.init(client,cubedb, conf);
		return cube;
	}
	void record(String action, String cubeName, String tuplename, String docid, Object value) {
		if ("qurey".equals(action))
			return;

		// 读取事务记录，并插入actions属性集合
		Bson filter = new BasicDBObject("_id", new ObjectId(id));
		FindIterable<Document> it = process.find(filter);
		Document d = it.first();
		if (d == null) {
			throw new EcmException("未发现事务开始");
		}
		process.updateOne(filter, Document.parse(String.format("{\"$set\":{\"state\":\"processing\"}}")));
		Document ins = new Document();
		ins.put("parent", id);
		ins.put("action", action);
		ins.put("cubeName", cubeName);
		ins.put("tupleName", tuplename);
		ins.put("tupleid", docid);

		MongoCollection<Document> col = client.getDatabase(cubeName).getCollection(tuplename);
		Bson nd = new BasicDBObject("_id", new ObjectId(docid));
		Document doc = col.find(nd).first();
		ins.put("tupleData", doc);// 保留文档可以在失败或重启后重试
		jobs.insertOne(ins);
	}

	@Override
	public void setContainsPenddingRows(boolean arg) {
		this.containsPenddingRows = arg;
	}

	@Override
	public boolean isContainsPenddingRows() {
		return containsPenddingRows;
	}

	@Override
	public void commit() {
		// 根据事务表的actions记录的表及id更新，去掉其isPendding属性
		// 将该事务表的记录的state更新为commit状态
		Bson filter = new BasicDBObject("_id", new ObjectId(id));
		FindIterable<Document> it = process.find(filter);
		Document d = it.first();
		if (d == null) {
			throw new EcmException("未发现事务开始");
		}

		process.updateOne(filter, Document.parse(String.format("{\"$set\":{\"state\":\"commiting\"}}")));

		Document doc = new Document();
		doc.put("parent", id);
		MongoCursor<Document> fi = jobs.find(doc).iterator();
		while (fi.hasNext()) {
			Document item = fi.next();
			String cubeName = item.getString("cubeName");
			String tupleName = item.getString("tupleName");
			String tupleid = item.getString("tupleid");
			MongoCollection<Document> tuple = client.getDatabase(cubeName).getCollection(tupleName);
			if (tuple != null) {
				Bson tid = new BasicDBObject("_id", new ObjectId(tupleid));
				Document update = new Document();
				Document u = new Document();
				u.put("isPendding", 1);
				update.put("$unset", u);
				tuple.updateOne(tid, update);
			}
		}
		process.updateOne(filter, Document.parse(String.format("{\"$set\":{\"state\":\"commited\"}}")));
		clearTran();
	}

	void clearTran() {
		Document filter1 = new Document();
		filter1.put("parent", id);
		jobs.deleteMany(filter1);
		Bson filter2 = new BasicDBObject("_id", new ObjectId(id));
		process.deleteOne(filter2);
	}

	@Override
	public void rollback() {
		// 根据事务表的actions记录的表及id删除
		// 将该事务表的记录的state更新为rollback状态
		Bson filter = new BasicDBObject("_id", new ObjectId(id));
		FindIterable<Document> it = process.find(filter);
		Document d = it.first();
		if (d == null) {
			throw new EcmException("未发现事务开始");
		}

		process.updateOne(filter, Document.parse(String.format("{\"$set\":{\"state\":\"rollbacking\"}}")));

		Document doc = new Document();
		doc.put("parent", id);
		MongoCursor<Document> fi = jobs.find(doc).iterator();
		while (fi.hasNext()) {
			Document item = fi.next();
			String cubeName = item.getString("cubeName");
			String tupleName = item.getString("tupleName");
			String tupleid = item.getString("tupleid");
			String action = item.getString("action");
			Document orgindoc = (Document) item.get("tupleData");
			MongoCollection<Document> tuple = client.getDatabase(cubeName).getCollection(tupleName);
			Bson tid = new BasicDBObject("_id", new ObjectId(tupleid));
			switch (action) {
			case "save":
				tuple.deleteOne(tid);
				break;
			case "update":
				Document nd = new Document("$set", orgindoc);
				tuple.updateOne(tid, nd);
				break;
			case "delete":
				tuple.insertOne(orgindoc);
				break;
			}
		}
		process.updateOne(filter, Document.parse(String.format("{\"$set\":{\"state\":\"rollbacked\"}}")));
		clearTran();
	}

	@Override
	public boolean isActive() {
		return false;
	}

}