package cj.lns.chip.sos.cube.framework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import cj.studio.ecm.CJSystem;
import cj.studio.ecm.EcmException;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.gson2.com.google.gson.reflect.TypeToken;
import cj.ultimate.util.StringUtil;

public class Cube implements ICube {
	protected MongoDatabase cubedb;
	protected MongoClient client;
	private ClassLoader classloader;
	public Cube(ClassLoader cl) {
		if(cl==null) {
			cl=this.getClass().getClassLoader();
		}
		this.classloader=cl;
	}

	@Override
	public String name() {
		return cubedb.getName();
	}
	
	public void init(MongoClient client,MongoDatabase cubedb, CubeConfig conf) {
		this.client=client;
		this.cubedb = cubedb;
		String systemDims = "system_dims";
		String systemCoords = "system_coordinates";
		MongoCollection<Document> coldims = cubedb.getCollection(systemDims);
		if (coldims != null) {
			coldims.drop();
		}
		CreateCollectionOptions options = new CreateCollectionOptions();
		cubedb.createCollection(systemDims, options);
		if (!StringUtil.isEmpty(conf.dimFile)) {
			saveDims(coldims, conf.dimFile);
		}
		MongoCollection<Document> colcoords = cubedb.getCollection(systemCoords);
		if (colcoords != null) {
			colcoords.drop();
		}
		CreateCollectionOptions optionscoord = new CreateCollectionOptions();
		cubedb.createCollection(systemCoords, optionscoord);
		if (!StringUtil.isEmpty(conf.getCoordinateFile())) {
			saveCoordinates(colcoords, conf.getCoordinateFile());
		}
		IDocument<CubeConfig> doc = new TupleDocument<CubeConfig>(conf);
		saveDoc(null, "system_config", doc, true);

		FileSystem fs = new FileSystem(this);
		fs.init(conf);
	}

	private void saveCoordinates(MongoCollection<Document> col, String coordinateFile) {
		FileReader in = null;
		BufferedReader reader = null;
		try {
			in = new FileReader(coordinateFile);
			reader = new BufferedReader(in);
			String tmp = "";
			while ((tmp = reader.readLine()) != null) {
				Document doc = Document.parse(tmp);
				col.insertOne(doc);
			}
		} catch (IOException e) {
			throw new EcmException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public void importCoordinates(String coordFile) {
		String systemCoords = "system_coordinates";
		MongoCollection<Document> col = cubedb.getCollection(systemCoords);
		saveCoordinates(col, coordFile);
	}

	@Override
	public void exportCoordinates(String coordFile) {
		String systemDims = "system_coordinates";
		MongoCollection<Document> col = cubedb.getCollection(systemDims);
		FindIterable<Document> find = col.find();
		FileWriter out = null;
		BufferedWriter writer = null;
		try {
			out = new FileWriter(coordFile);
			writer = new BufferedWriter(out);
			for (Document dimDoc : find) {
				writer.write(dimDoc.toJson());
			}
		} catch (Exception e) {
			throw new EcmException(e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public void exportDims(String dimFile) {
		String systemDims = "system_dims";
		MongoCollection<Document> col = cubedb.getCollection(systemDims);
		Bson filter = Document.parse("{'_dimension':{$exists:true}}");
		FindIterable<Document> find = col.find(filter);
		FileWriter out = null;
		BufferedWriter writer = null;
		try {
			out = new FileWriter(dimFile);
			writer = new BufferedWriter(out);
			for (Document dimDoc : find) {
				writer.write(dimDoc.toJson());
			}
		} catch (Exception e) {
			throw new EcmException(e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public void importOneDimByJson(String json) {
		String systemDims = "system_dims";
		MongoCollection<Document> col = cubedb.getCollection(systemDims);
		Document doc = Document.parse(json);
		if (!doc.containsKey("_dimension")) {
			extracted();
		}
		Document def = (Document) doc.get("_dimension");
		String dimName = def.getString("name");
		if (existsDimension(dimName)) {
			CJSystem.current().environment().logging().warn(String.format("维度已存在，故未导入此维：%s", dimName));
		} else {
			col.insertOne(doc);
		}
	}

	@Override
	public void importDims(String dimFile) {
		String systemDims = "system_dims";
		MongoCollection<Document> col = cubedb.getCollection(systemDims);
		saveDims(col, dimFile);
	}

	private void saveDims(MongoCollection<Document> col, String dimFile) {
		FileReader in = null;
		BufferedReader reader = null;
		try {
			in = new FileReader(dimFile);
			reader = new BufferedReader(in);
			String tmp = "";
			while ((tmp = reader.readLine()) != null) {
				Document doc = Document.parse(tmp);
				if (!doc.containsKey("_dimension")) {
					extracted();
				}
				Document def = (Document) doc.get("_dimension");
				String dimName = def.getString("name");
				if (existsDimension(dimName)) {
					CJSystem.current().environment().logging().warn(String.format("维度已存在，故未导入此维：%s", dimName));
				} else {
					col.insertOne(doc);
				}
			}
		} catch (IOException e) {
			throw new EcmException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}

	}

	private void extracted() {
		throw new EcmException("维度定义缺少：_dimension对象");
	}

	@Override
	public Dimension dimension(String name) {
		MongoCollection<Document> col = cubedb.getCollection("system_dims");
		Bson filter = Document.parse(String.format("{'_dimension.name':'%s'}", name));
		FindIterable<Document> find = col.find(filter);
		Document doc = find.first();
		if (doc == null)
			return null;
		Document dimDesc = (Document) doc.get("_dimension");
		Dimension dim = new Dimension(name);
		dim.alias = dimDesc.getString("alias");
		dim.desc = dimDesc.getString("desc");
		dim.id = ((ObjectId) doc.get("_id")).toHexString();
		Hierarcky hier = dim.hierarcky();
		Set<String> set = doc.keySet();
		for (String key : set) {
			if (key.equals("_dimension")) {
				continue;
			}
			if (key.equals("_id")) {
				continue;
			}
			Document p = (Document) doc.get(key);
			Level level = new Level(new Property(key, p.getString("alias"), p.getString("dataType")));
			hier.appendLevel(level);
		}
		return dim;
	}

	@Override
	public List<String> enumDimension() {
		List<String> dims = new ArrayList<>();
		MongoCollection<Document> col = cubedb.getCollection("system_dims");
		Bson filter = Document.parse("{'_dimension':{$ne:null}}");
		FindIterable<Document> find = col.find(filter);
		find.projection(new BasicDBObject("_dimension.name", "1"));
		for (Document doc : find) {
			Document desc = (Document) doc.get("_dimension");
			dims.add(desc.getString("name"));
		}

		return dims;
	}

	@Override
	public void updateCubeConfig(CubeConfig config) {
		String cql = "select {'tuple':'*'} " + " from     tuple system_config   ?(tupleClass) ";
		IQuery<CubeConfig> q = createQuery(cql);
		q.setParameter("tupleClass", CubeConfig.class.getName());
		IDocument<CubeConfig> conf = q.getSingleResult();
		IDocument<CubeConfig> newconf = new TupleDocument<>(config);
		this.updateDoc("system_config", conf.docid(), newconf);
	}

	@Override
	public double dataSize() {
		Document doc = cubeStats();
		return convertToDouble(doc.get("dataSize"));
	}

	@Override
	public double dataSize(String tupleName) {
		Document doc = tupleStats(tupleName);
		return convertToDouble(doc.get("size"));
	}

	@Override
	public double usedSpace(String tupleName) {
		Document doc = this.tupleStats(tupleName);
		double i = 0;
		Object o = doc.get("totalIndexSize");
		i = convertToDouble(o);
		o = doc.get("storageSize");
		double j = 0;
		j = convertToDouble(o);
		return i + 0D + j;
	}

	static double convertToDouble(Object o) {
		double j = 0;
		if (o instanceof Integer) {
			j = (int) o;
		} else if (o instanceof Long) {
			j = (long) o;
		} else if (o instanceof Double) {
			j = (double) o;
		} else {
			throw new EcmException("未实现类型");
		}
		return j;
	}

	@Override
	public double usedSpace() {// storageSize+totalIndexSize
		Document doc = cubeStats();
		double j = 0;
		Object o = doc.get("storageSize");
		j = convertToDouble(o);
		return j;
	}

	@Override
	public CubeConfig config() {
		String cql = "select {'tuple':'*'} " + " from     tuple system_config   ?(tupleClass) ";
		IQuery<CubeConfig> q = createQuery(cql);
		q.setParameter("tupleClass", CubeConfig.class.getName());
		IDocument<CubeConfig> conf = q.getSingleResult();
		if (conf == null)
			return null;
		return conf.tuple();
	}

	@Override
	public Document cubeStats() {
		BsonDocument commandDocument = new BsonDocument("dbStats", new BsonInt32(1)).append("scale", new BsonInt32(1));
		Document stats = cubedb.runCommand(commandDocument);
		return stats;
	}

	@Override
	public Document tupleStats(String tupleName) {
		BsonDocument bd = new BsonDocument("collStats", new BsonString(tupleName));
		Document bddoc = cubedb.runCommand(bd);
		return bddoc;
	}

	@Override
	public boolean existsDimension(String name) {
		MongoCollection<Document> col = cubedb.getCollection("system_dims");
		Bson filter = Document.parse(String.format("{'_dimension.name':'%s'}", name));
		long size = col.count(filter);
		return size > 0;
	}

	@Override
	public void saveDimension(Dimension dim) {
		if (existsDimension(dim.name)) {
			throw new EcmException("已存在维度：" + dim.name);
		}
		String json = dim.toBson();
		MongoCollection<Document> col = cubedb.getCollection("system_dims");
		Document document = Document.parse(json);
		col.insertOne(document);
	}

	@Override
	public void removeDimension(String name) {
		try {
			MongoCollection<Document> col = cubedb.getCollection("system_dims");
			Bson filter = Document.parse(String.format("{'_dimension.name':'%s'}", name));
			col.deleteOne(filter);
		} catch (Exception e) {
			throw new EcmException(e);
		}
	}

	@Override
	public List<Coordinate> rootCoordinates(String dimName) {
		MongoCollection<Document> col = cubedb.getCollection("system_coordinates");
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return new ArrayList<>();
		}
		Level head = dim.hierarcky().head();
		if (head == null) {
			return new ArrayList<>();
		}
		return findCoordinates(col, dimName, head, null);
	}

	@Override
	public List<Coordinate> childCoordinates(String dimName, Coordinate coordinate) {
		MongoCollection<Document> col = cubedb.getCollection("system_coordinates");
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return new ArrayList<>();
		}
		String propName = coordinate.propName();
		Level level = dim.hierarcky().level(propName);
		if (level == null) {
			return new ArrayList<>();
		}
		level = level.nextLevel();
		return findCoordinates(col, dimName, level, coordinate);
	}

	@Override
	public List<Coordinate> rootCoordinates(String tupleName, String dimName) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return new ArrayList<>();
		}
		Level head = dim.hierarcky().head();
		if (head == null) {
			return new ArrayList<>();
		}
		return findCoordinates(col, dimName, head, null);
	}

	@Override
	public List<Coordinate> childCoordinates(String tupleName, String dimName, Coordinate coordinate) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return new ArrayList<>();
		}
		String propName = coordinate.propName();
		Level level = dim.hierarcky().level(propName);
		if (level == null) {
			return new ArrayList<>();
		}
		level = level.nextLevel();
		return findCoordinates(col, dimName, level, coordinate);
	}

	private List<Coordinate> findCoordinates(MongoCollection<Document> col, String dimName, Level level,
			Coordinate coordinate) {
		Dimension dim = dimension(dimName);
		if (dim == null) {
			throw new EcmException("维度不存在：" + dimName);
		}
		if (level == null) {
			return new ArrayList<>();
		}

		Property levelprop = level.property();

		String json = null;
		if (level.parent() == null) {
			json = String.format("{'%s':{$ne:null}}", dimName);
		} else {
			Property plp = level.parent().property;
			Coordinate c = coordinate;
			while (c != null) {
				if (plp.name.equals(c.propName())) {
					break;
				}
				c = c.nextLevel();
			}
			if (String.class.getName().equals(plp.dataType)) {
				json = String.format("{'%s.%s':'%s'}", dimName, plp.name, c.value());
			} else {
				json = String.format("{'%s.%s':%s}", dimName, plp.name, c.value());
			}
		}
		Bson filter = Document.parse(json);
		Class<?> dataType = null;
		try {
			dataType = Class.forName(levelprop.dataType);
		} catch (ClassNotFoundException e) {
			throw new EcmException(String.format("找不到属性的数据类型：%s", e));
		}

		DistinctIterable<?> dist = col.distinct(String.format("%s.%s", dimName, levelprop.getName()), filter, dataType);
		MongoCursor<?> it = dist.iterator();
		List<Coordinate> list = new ArrayList<>();
		while (it.hasNext()) {
			Object o = it.next();
			Coordinate coord = new Coordinate(levelprop.getName(), o);
			coord.parentLevel = coordinate;
			list.add(coord);
		}
		return list;
	}

	/**
	 * 多维查询
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param tupleName
	 * @param tupleClazz
	 * @param coordinates
	 *            维度坐标集合，key是维度名，一个维度只能传入一个坐标 <br>
	 *            注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @return
	 */
	@Override
	public <T> List<IDocument<T>> listTuplesByCoordinate(String tupleName, Class<T> tupleClazz,
			Map<String, Coordinate> coordinates, boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			combineWhereByCoordinate(dimName, coordinate, replace, lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} " + " from tuple ?(tupleName) ?(tupleClassName) " + " where {?(replace)}";
		IQuery<T> q = createQuery(cql);
		q.setParameter("tupleName", tupleName);
		q.setParameter("tupleClassName", tupleClazz.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<T>> docs = q.getResultList();
		return docs;
	}

	/**
	 * 多维查询
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param tupleName
	 * @param tupleClazz
	 * @param coordinates
	 *            维度坐标集合，key是维度名，一个维度只能传入一个坐标 <br>
	 *            注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @return
	 */
	@Override
	public <T> List<IDocument<T>> listTuplesByCoordinate(String tupleName, Class<T> tupleClazz,
			Map<String, Coordinate> coordinates, String sortBson, int skip, int limit, boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			combineWhereByCoordinate(dimName, coordinate, replace, lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		if (StringUtil.isEmpty(sortBson)) {
			sortBson = "{}";
		}
		if (limit < 0) {
			limit = Integer.MAX_VALUE;
		}
		if (skip < 0) {
			skip = 0;
		}
		String cql = String.format(
				"select {'tuple':'*'}.limit(%d).skip(%d).sort(%s) from tuple ?(tupleName) ?(tupleClassName) where {?(replace)}",
				limit, skip, sortBson);
		IQuery<T> q = createQuery(cql);
		q.setParameter("tupleName", tupleName);
		q.setParameter("tupleClassName", tupleClazz.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<T>> docs = q.getResultList();
		return docs;
	}

	// 注意：该方法返回的字串replace尾可能包括了,号
	/**
	 * 拼合坐标作为查询条件
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param dimName
	 * @param coordinate
	 *            注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。<br>
	 *            注意坐标值的数据类型，如果类型不对，根据mongodb的bson规则就查不出来。<br>
	 *            注意要与维度级别属性定义的类型匹配，该方法根据级别属性定义的类型对坐标成员值进行了简单的转换，转换的类型有：java.
	 *            lang.String,java.lang.Integer,java.lang.Long
	 * @param replace
	 * @param lookAll
	 *            每级是否包括全部子级及所有孙级成员的查询，false只查询本级的直接成员，如同目录只列出其直接包含的文件。
	 */
	@Override
	public void combineWhereByCoordinate(String dimName, Coordinate coordinate, StringBuffer replace, boolean lookAll) {
		if (!existsDimension(dimName)) {
			throw new EcmException(String.format("维度不存在：%s", dimName));
		}
		coordinate = coordinate.depCopyFromEnd();
		Dimension dim = dimension(dimName);
		Hierarcky hier = dim.hierarcky();

		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			Level level = hier.level(tmp.propName());
			if (level == null) {
				throw new EcmException(String.format("输入的坐标的维度：%s 的属性%s级别不存在", dimName, tmp.propName()));
			}
			if (level.isDataType(String.class)) {
				replace.append(String.format("'%s.%s':'%s',", dimName, tmp.propName(), tmp.value()));
			} else if (level.isDataType(Integer.class)) {
				int v = 0;
				if (tmp.value() instanceof Integer) {
					v = (int) tmp.value();
				} else {
					v = Integer.valueOf((String) tmp.value());
				}
				replace.append(String.format("'%s.%s':%s,", dimName, tmp.propName(), v));
			} else if (level.isDataType(Long.class)) {
				long v = 0;
				if (tmp.value() instanceof Integer) {
					v = (long) tmp.value();
				} else {
					v = Long.valueOf((String) tmp.value());
				}
				replace.append(String.format("'%s.%s':%s,", dimName, tmp.propName(), v));
			} else {
				replace.append(String.format("'%s.%s':%s,", dimName, tmp.propName(), tmp.value()));
			}

			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		if (!lookAll) {
			Level next = hier.level(last.propName()).nextLevel();
			if (next != null) {
				replace.append(String.format("'%s.%s':{%s},", dimName, next.property.name, "$exists:false"));
			}
		}
	}

	/**
	 * 按一个指定的维度查询
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param tupleName
	 * @param tupleClazz
	 * @param dimName
	 * @param coordinate
	 *            注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @return
	 */
	@Override
	public <T> List<IDocument<T>> listTuplesByCoordinate(String tupleName, Class<T> tupleClazz, String dimName,
			Coordinate coordinate, boolean lookAll) {
		StringBuffer replace = new StringBuffer();
		combineWhereByCoordinate(dimName, coordinate, replace, lookAll);

		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} " + " from tuple ?(tupleName) ?(tupleClassName) " + " where {?(replace)}";
		IQuery<T> q = createQuery(cql);
		q.setParameter("tupleName", tupleName);
		q.setParameter("tupleClassName", tupleClazz.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<T>> docs = q.getResultList();
		return docs;
	}

	/**
	 * 按一个指定的维度查询
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param tupleName
	 * @param tupleClazz
	 * @param dimName
	 * @param coordinate
	 *            注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @param limit
	 *            如果小于0则为不受限，所回所有
	 * @return
	 */
	@Override
	public <T> List<IDocument<T>> listTuplesByCoordinate(String tupleName, Class<T> tupleClazz, String dimName,
			Coordinate coordinate, String sortBson, int skip, int limit, boolean lookAll) {
		StringBuffer replace = new StringBuffer();
		combineWhereByCoordinate(dimName, coordinate, replace, lookAll);

		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		if (StringUtil.isEmpty(sortBson)) {
			sortBson = "{}";
		}
		if (limit < 0) {
			limit = Integer.MAX_VALUE;
		}
		if (skip < 0) {
			skip = 0;
		}
		String cql = String.format(
				"select {'tuple':'*'}.limit(%s).skip(%s).sort(%s) from tuple ?(tupleName) ?(tupleClassName)  where {?(replace)}",
				limit, skip, sortBson);
		IQuery<T> q = createQuery(cql);
		q.setParameter("tupleName", tupleName);
		q.setParameter("tupleClassName", tupleClazz.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<T>> docs = q.getResultList();
		return docs;
	}

	@Override
	public long listTuplesByCoordinate(String tupleName, String dimName, Coordinate coordinate, boolean lookAll) {
		StringBuffer replace = new StringBuffer();
		combineWhereByCoordinate(dimName, coordinate, replace, lookAll);

		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count() " + " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {?(replace)}";
		IQuery<Long> q = createQuery(cql);
		q.setParameter("tupleName", tupleName);
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("replace", replace.toString());

		return q.count();
	}

	@Override
	public boolean existsCoordinate(String dimName, String propName, Object member) {
		MongoCollection<Document> col = cubedb.getCollection("system_coordinates");
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return false;
		}
		Hierarcky hier = dim.hierarcky();
		Level level = hier.level(propName);
		if (level == null) {
			return false;
		}
		if (!member.getClass().getName().equals(level.property().dataType)) {
			throw new EcmException(String.format("成员类型不匹配。维度%s需要的类型是：%s", dimName, level.property.dataType));
		}
		String json = "";
		if (String.class.getName().equals(level.property().dataType)) {
			json = String.format("{'%s.%s':'%s'}", dimName, propName, member);
		} else {
			json = String.format("{'%s.%s':%s}", dimName, propName, member);
		}

		Bson bson = Document.parse(json);
		return col.count(bson) > 0;
	}

	@Override
	public void removeCoordinate(String dimName, String propName, Object member) {
		MongoCollection<Document> col = cubedb.getCollection("system_coordinates");
		Dimension dim = dimension(dimName);
		if (dim == null) {
			return;
		}
		Hierarcky hier = dim.hierarcky();
		Level level = hier.level(propName);
		if (level == null) {
			return;
		}
		if (!member.getClass().getName().equals(level.property().dataType)) {
			throw new EcmException(String.format("成员类型不匹配。维度%s需要的类型是：%s", dimName, level.property.dataType));
		}
		String json = "";
		if (String.class.getName().equals(level.property().dataType)) {
			json = String.format("{'%s.%s':'%s'}", dimName, propName, member);
		} else {
			json = String.format("{'%s.%s':%s}", dimName, propName, member);
		}

		Bson bson = Document.parse(json);
		col.deleteMany(bson);
	}

	@Override
	public void saveCoordinate(String dimName, Coordinate coord) {
		Coordinate last = coord;
		while (last.nextLevel() != null) {
			last = last.nextLevel();
		}
		if (existsCoordinate(dimName, last.propName(), last.value())) {
			throw new EcmException(String.format("维度%s中存在成员：%s[%s]", dimName, last.propName(), last.value()));
		}
		Dimension dim = dimension(dimName);
		if (dim == null) {
			throw new EcmException(String.format("维度%s不存在", dimName));
		}
		Hierarcky hier = dim.hierarcky();
		MongoCollection<Document> col = cubedb.getCollection("system_coordinates");
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("{'%s':{", dimName));
		Coordinate tmp = coord;
		while (tmp != null) {
			Level level = hier.level(tmp.propName());
			if (level == null) {
				throw new EcmException(String.format("维度%s中不存在属性：%s", dimName, tmp.propName()));
			}
			if (!tmp.value().getClass().getName().equals(level.property().dataType)) {
				throw new EcmException(String.format("成员类型不匹配。维度%s需要的类型是：%s", dimName, level.property.dataType));
			}
			if (tmp.value() instanceof String) {
				sb.append(String.format("'%s':'%s',", tmp.propName(), tmp.value()));
			} else {
				sb.append(String.format("'%s':%s,", tmp.propName(), tmp.value()));
			}
			tmp = tmp.nextLevel();
		}
		if (sb.charAt(sb.length() - 1) == ',') {
			sb.deleteCharAt(sb.length() - 1);
		}
		sb.append("}}");
		Document document = Document.parse(sb.toString());
		col.insertOne(document);
	}

	@Override
	public void empty() {
		List<Coordinate> list = tupleCoordinate();
		for (Coordinate c : list) {
			cubedb.getCollection((String) c.value()).drop();
		}
	}

	@Override
	public Map<String, Coordinate> parseCoordinate(String json) {
		Map<String, String> map = new Gson().fromJson(json, new TypeToken<HashMap<String, String>>() {
		}.getType());
		Set<String> set = map.keySet();
		Map<String, Coordinate> coords = new HashMap<>();
		for (String dimName : set) {
			String coord = map.get(dimName);
			switch (dimName) {
			case "dir":
				dimName = "system_fs_directories";
				break;
			case "fileType":
				dimName = "system_fs_files";
				break;
			default:
				break;
			}
			Dimension dim = dimension(dimName);
			if (dim == null) {
				throw new EcmException(String.format("维度未定义:'%s'", dimName));
			}
			Hierarcky hier = dim.hierarcky();
			String mem[] = coord.split("/");
			int i = 0;
			Coordinate dimCoord = null;
			if ("system_fs_directories".equals(dimName)) {
				Level level = hier.level(0);
				dimCoord = new Coordinate(level.levelName(), "/");
				i++;// 级别路过－1
			}
			for (String v : mem) {
				if (StringUtil.isEmpty(v)) {
					continue;
				}
				Level level = hier.level(i);
				if (level == null) {
					throw new EcmException(String.format("维度中未有相应级别层级:'%s'", i));
				}
				Coordinate newCoord = new Coordinate(level.levelName(), v);
				if (dimCoord == null) {
					dimCoord = newCoord;
				} else {
					dimCoord = dimCoord.nextLevel(newCoord);
				}
				i++;
			}
			if (dimCoord != null) {
				Coordinate end = dimCoord.goLast();
				coords.put(dimName, end);
			}

		}
		return coords;
	}

	@Override
	public List<Coordinate> tupleCoordinate() {
		List<Coordinate> list = new ArrayList<>();
		MongoIterable<String> it = cubedb.listCollectionNames();
		for (String n : it) {
			if (n.startsWith("system_") || n.startsWith("system.")) {
				continue;
			}
			Coordinate t = new Coordinate("tupleName", n);
			list.add(t);
		}
		return list;
	}

	@Override
	public Dimension tupleDimension() {
		Dimension dim = new Dimension("tuple");
		dim.alias = "元组";
		dim.desc = "立方体的元组集合";
		Hierarcky hier = dim.hierarcky();
		hier.setHead(new Level(new Property("tupleName", "元组名", String.class.getName())));
		return dim;
	}

	@Override
	public <T> IQuery<T> count(String cubeql) {
		IQuery<T> q = new CubeQlQuery<T>(this, cubeql,classloader);
		return q;
	}

	@Override
	public <T> AggregateIterable<T> aggregate(String tupleName, List<? extends Bson> pipeline, Class<T> resultClass) {
		return cubedb.getCollection(tupleName).aggregate(pipeline, resultClass);
	}

	@Override
	public AggregateIterable<Document> aggregate(String tupleName, List<? extends Bson> pipeline) {
		return cubedb.getCollection(tupleName).aggregate(pipeline);
	}

	@Override
	public <T> IQuery<T> createQuery(String cubeql) {
		IQuery<T> q = new CubeQlQuery<T>(this, cubeql,classloader);
		return q;
	}
	
	@Override
	public <T> IDocument<T> document(String colname, String docid, Class<T> clazz) {
		String cjql = "select {'tuple':'*'} from tuple ?(colname) ?(clazz) where {'_id':ObjectId('?(docid)')}";
		IQuery<T> q = createQuery(cjql);
		q.setParameter("colname", colname);
		q.setParameter("clazz", clazz.getName());
		q.setParameter("docid", docid);
		return q.getSingleResult();
	}

	@Override
	public long tupleCount(String colname) {
		String cjql = "select {'tuple':'*'}.count() from tuple ?(colname) java.lang.Long where {}";
		IQuery<Long> q = createQuery(cjql);
		q.setParameter("colname", colname);
		return q.count();
	}

	@Override
	public long tupleCount(String colname, String where) {
		String cjql = String.format("select {'tuple':'*'}.count() from tuple ?(colname) java.lang.Long where %s",
				where);
		IQuery<Long> q = createQuery(cjql);
		q.setParameter("colname", colname);
		return q.count();
	}

	@Override
	public void close() {
		this.cubedb = null;
	}

	@Override
	public String createIndex(String tupleName, Bson keys) {
		MongoCollection<Document> col = this.cubedb.getCollection(tupleName);
		return col.createIndex(keys);
	}

	@Override
	public String createIndex(String tupleName, Bson keys, IndexOptions indexOptions) {
		MongoCollection<Document> col = this.cubedb.getCollection(tupleName);
		return col.createIndex(keys, indexOptions);
	}

	@Override
	public List<String> createIndexes(String tupleName, List<IndexModel> indexes) {
		MongoCollection<Document> col = this.cubedb.getCollection(tupleName);
		return col.createIndexes(indexes);
	}

	@Override
	public String saveDoc(String tupleName, IDocument<?> doc) {
		return saveDoc(null, tupleName, doc, false);
	}

	String saveDoc(ITranscation tran, String tupleName, IDocument<?> doc, boolean isSystemCol) {
		if (!isSystemCol && tupleName.startsWith("system_")) {
			throw new EcmException(String.format("system_前缀是立方体系统集合,参数：%s", tupleName));
		}
		Calendar cal = Calendar.getInstance();
		String dimName = "createDate";
		Coordinate year = new Coordinate("year", cal.get(Calendar.YEAR));
		Coordinate month = new Coordinate("month", cal.get(Calendar.MONTH) + 1);
		Coordinate day = new Coordinate("day", cal.get(Calendar.DAY_OF_MONTH));
		year.nextLevel(month);
		month.nextLevel(day);

		doc.addCoordinate(dimName, year);

		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		// if (col.count() < 1) {
		// cubedb.createCollection(tupleName);
		// col = cubedb.getCollection(tupleName);
		// }
		Document document = convertDoc(doc);
		int size = document.size();// 判断是否超出立方体空间容量
		double totalSize = size + this.usedSpace();
		CubeConfig conf = config();
		if (conf != null) {
			double capacity = conf.capacity;
			if (capacity >= 0 && totalSize >= capacity) {
				throw new EcmException("存储空间已满.");
			}
		}
		if (tran != null) {
			document.put("isPendding", true);
		}
		col.insertOne(document);

		ObjectId oid = document.getObjectId("_id");
		if (oid != null) {
			String id = oid.toHexString();
			if (doc instanceof TupleDocument) {
				((TupleDocument<?>) doc).id = id;
			}
			if (tran != null) {
				Transcation tn = (Transcation) tran;
				tn.record("save", name(),tupleName, id, document);
			}
			return id;
		}
		return null;
	}

	@Override
	public String saveDoc(ITranscation tran, String tupleName, IDocument<?> doc) {
		return saveDoc(tran, tupleName, doc, false);
	}

	private Document convertDoc(IDocument<?> doc) {
		Document raw = new Document();
		String[] coordinates = doc.enumCoordinate();
		for (String name : coordinates) {
			if (name.equals("tuple")) {
				throw new EcmException(String.format("坐标的维度名不能是tuple"));
			}
			Coordinate coor = doc.coordinate(name);
			Document coorRaw = new Document();
			Coordinate tmp = coor;
			do {
				coorRaw.append(tmp.propName(), tmp.value());
				tmp = tmp.nextLevel();
			} while (tmp != null);
			raw.append(name, coorRaw);
		}

		String json = new Gson().toJson(doc.tuple());
		Document tuple = Document.parse(json);
		raw.append("tuple", tuple);
		return raw;
	}

	@Override
	public UpdateResult updateDocs(String tupleName, Bson filter, Bson update) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		UpdateOptions updateOptions = new UpdateOptions();
		updateOptions.upsert(false);
		UpdateResult result = col.updateMany(filter, update, updateOptions);
		return result;
	}

	@Override
	public UpdateResult updateDocs(String tupleName, Bson filter, Bson update, UpdateOptions updateOptions) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		UpdateResult result = col.updateMany(filter, update, updateOptions);
		return result;
	}

	@Override
	public UpdateResult updateDocOne(String tupleName, Bson filter, Bson update) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		UpdateOptions updateOptions = new UpdateOptions();
		updateOptions.upsert(false);
		UpdateResult result = col.updateOne(filter, update, updateOptions);
		return result;
	}

	@Override
	public UpdateResult updateDocOne(String tupleName, Bson filter, Bson update, UpdateOptions updateOptions) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		UpdateResult result = col.updateOne(filter, update, updateOptions);
		return result;
	}

	@Override
	public UpdateResult updateDoc(ITranscation tran, String tupleName, String docid, IDocument<?> newdoc) {
		return updateDocImpl(tran, tupleName, docid, newdoc, null);
	}

	@Override
	public UpdateResult updateDoc(ITranscation tran, String tupleName, String docid, IDocument<?> newdoc,
			UpdateOptions options) {
		return updateDocImpl(tran, tupleName, docid, newdoc, options);
	}

	@Override
	public UpdateResult updateDoc(String tupleName, String docid, IDocument<?> newdoc) {
		return updateDocImpl(null, tupleName, docid, newdoc, null);
	}

	@Override
	public UpdateResult updateDoc(String tupleName, String docid, IDocument<?> newdoc, UpdateOptions options) {
		return updateDocImpl(null, tupleName, docid, newdoc, options);
	}

	UpdateResult updateDocImpl(ITranscation tran, String tupleName, String docid, IDocument<?> newdoc,
			UpdateOptions options) {
		if (StringUtil.isEmpty(docid)) {
			throw new EcmException("未有指定要更新的DOCID");
		}
		Calendar cal = Calendar.getInstance();
		String dimName = "updateDate";
		Coordinate year = new Coordinate("year", cal.get(Calendar.YEAR));
		Coordinate month = new Coordinate("month", cal.get(Calendar.MONTH) + 1);
		Coordinate day = new Coordinate("day", cal.get(Calendar.DAY_OF_MONTH));
		year.nextLevel(month);
		month.nextLevel(day);
		newdoc.removeCoordinate(dimName);
		newdoc.addCoordinate(dimName, year);

		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		ObjectId id = new ObjectId(docid);
		Bson bson = new BasicDBObject("_id", id);
		TupleDocument<?> td = (TupleDocument<?>) newdoc;
		td.id = docid;
		Document doc = convertDoc(newdoc);
		Document nd = new Document("$set", doc);
		if (tran != null) {
			Transcation t = (Transcation) tran;
			t.record("update", name(), tupleName, docid,null);
		}
		// col.replaceOne(bson, doc);
		if (options == null) {
			return col.updateOne(bson, nd);
		} else {
			return col.updateOne(bson, nd, options);
		}
	}

	@Override
	public void deleteDoc(String tupleName, IDocument<?> doc) {
		deleteDoc(tupleName, doc.docid());
	}

	@Override
	public void deleteDoc(String tupleName, String docid) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		ObjectId id = new ObjectId(docid);
		Bson bson = new BasicDBObject("_id", id);
		col.deleteOne(bson);
	}

	@Override
	public void dropTuple(String tupleName) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		col.drop();
	}

	@Override
	public long deleteDocs(String tupleName, String whereBson) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		Bson bson = Document.parse(whereBson);
		DeleteResult result = col.deleteMany(bson);
		return result.getDeletedCount();
	}

	@Override
	public long deleteDocOne(String tupleName, String whereBson) {
		MongoCollection<Document> col = cubedb.getCollection(tupleName);
		Bson bson = Document.parse(whereBson);
		DeleteResult result = col.deleteOne(bson);
		return result.getDeletedCount();
	}

	private void load(MongoClient client,MongoDatabase cubedb) {
		this.cubedb = cubedb;
		this.client = client;
	}

	@Override
	public void deleteCube() {
		cubedb.drop();
	}
	public static ICube open(MongoClient client, String name) {
		return open(client, name, Cube.class.getClassLoader());
	}
	public static ICube open(MongoClient client, String name,ClassLoader cl) {
		Cube cube = new Cube(cl);
		cube.load(client,client.getDatabase(name));
		return cube;
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

	public static ICube create(MongoClient client, String name, CubeConfig conf,ClassLoader cl) {
		Cube cube = new Cube(cl);
		MongoDatabase cubedb = client.getDatabase(name);
		cube.init(client,cubedb, conf);
		return cube;
	}

	@Override
	public boolean isEmpty() {
		return tupleCoordinate().isEmpty();
	}

	@Override
	public FileSystem fileSystem() {
		return new FileSystem(this);
	}

	@Override
	public <T> IQuery<T> count(ITranscation tran, String cubeql) {
		IQuery<T> q = new CubeQlQuery<T>(this, tran, cubeql,classloader);
		return q;
	}

	@Override
	public <T> IQuery<T> createQuery(ITranscation tran, String cubeql) {
		IQuery<T> q = new CubeQlQuery<T>(this, tran, cubeql,classloader);
		return q;
	}

	@Override
	public void deleteDoc(ITranscation tran, String tupleName, String docid) {
		if (tran != null) {
			Transcation t = (Transcation) tran;
			t.record("delete", name(), tupleName, docid, null);
		}
		deleteDoc(tupleName, docid);
	}

	@Override
	public <T> IDocument<T> document(ITranscation tran, String colname, String docid, Class<T> clazz) {
		IDocument<T> doc = document(null, colname, docid, clazz);
		if (tran != null) {
			if (doc != null && doc.isPendding()) {
				return null;
			}
		}
		return doc;
	}

	@Override
	public long tupleCount(ITranscation tran, String colname) {
		String cjql = "select {'tuple':'*'}.count() from tuple ?(colname) java.lang.Long where {}";
		IQuery<Long> q = createQuery(tran, cjql);
		q.setParameter("colname", colname);
		return q.count();
	}

	@Override
	public long tupleCount(ITranscation tran, String colname, String where) {
		String cjql = String.format("select {'tuple':'*'}.count() from tuple ?(colname) java.lang.Long where %s",
				where);
		IQuery<Long> q = createQuery(tran, cjql);
		q.setParameter("colname", colname);
		return q.count();
	}

	@Override
	public ITranscation begin() {
		Transcation tran = new Transcation(this);
		tran.init();
		return tran;
	}
}
