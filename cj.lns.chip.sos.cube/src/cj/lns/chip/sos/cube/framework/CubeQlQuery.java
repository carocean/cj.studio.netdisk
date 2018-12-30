package cj.lns.chip.sos.cube.framework;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CountOptions;

import cj.studio.ecm.EcmException;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.util.StringUtil;

class CubeQlQuery<T> implements IQuery<T> {
	private Cube cube;
	String tupleClazz; // 要返射的元组类型
	String tupleName;// 作为mongodb collection名
	String whereBson;
	String tupleFieldBson;
	boolean hasCountFun;
	String skip;// 表示没有跳过
	String limit;// 表示没有限制
	Pattern pattern;
	Map<String, Object> parameters;
	Pattern paramPatt;
	private String sortbson;
	private boolean hasDistinctFun;
	private ITranscation transcation;
	private ClassLoader classloader;
	// private Class<?> distinctClazz;

	public CubeQlQuery(ICube cube, String cubeql,ClassLoader classLoader) {
		this(cube,null,cubeql,classLoader);
	}
	public CubeQlQuery(ICube cube,ITranscation tran, String cubeql,ClassLoader classLoader) {
		if(classLoader==null) {
			classLoader=this.getClass().getClassLoader();
		}
		this.classloader=classLoader;
		this.cube = (Cube) cube;
		this.parameters = new HashMap<>();
		pattern = Pattern.compile(
				"^\\s*select\\s+(.+)\\s+from\\s+tuple\\s+(.+)\\s+(where\\s+(.+)\\s*)*$");
		paramPatt = Pattern.compile("\\?\\((\\w+)\\)");
		this.transcation=tran;
		parse(cubeql);
	}
	/**
	 * <pre>
	 *  语法：
	select x from tuple class where y 返回列表
	select x.count() from tuple classs where y    返回一个整数值
	select x.skip(n).limit(n) from tuple class where y 返回列表
	select x.sort(z).skip(n).limit(n) from tuple class where y 返回列表
	
	注：其中x,y,z均为bson对象，x是元组对象的bson，即花括号括起来{},x后面可跟函数。n为数值。class是元组对象的类全名，如博客类cj.my.Blog
	tuple后是元组对象类
	
	例：
	select {'tuple.name':1,'tuple.content':1}.count() from tuple cj.my.Blog where {'date.year':'2006','date.month':4,'creator.user':'zhao','location.country':'zh','location.city':'zhangzhou','tuple.name':'发布'}
	
	其中：date,creator是维度名，.号之后是属性名，where语句中的tuple.name是将元组对象cj.my.Blog当维度使用
	select {'tuple':'*'} from ...
	 * </pre>
	 */
	protected void parse(String cubeql) {
		Matcher m = pattern.matcher(cubeql);
		if (!m.matches() || (m.groupCount() != 4)) {
			throw new EcmException(
					"语法错误，格式：select bson from tuple tupleName className where bson");
		}
		String from = m.group(2).trim();
		Pattern fromReg = Pattern.compile("\\s*(\\S+)\\s+(\\S+)\\s*");
		Matcher formm = fromReg.matcher(from);
		if (!formm.matches()) {
			throw new EcmException(
					"form语法错误，格式：from tuple tupleName className");
		}
		this.tupleName = formm.group(1);
		this.tupleClazz = formm.group(2);
		this.whereBson = m.group(4);
		String tupleStr = m.group(1).trim();
		int lastpos = tupleStr.indexOf("}", 1);
		this.tupleFieldBson = tupleStr.substring(tupleStr.indexOf("{"),
				lastpos + 1);
		if (lastpos + 1 > tupleStr.length()) {
			return;
		}
		if (lastpos + 1 >= tupleStr.length()) {// 后面没有函数
			return;
		}
		String funs = tupleStr.substring(lastpos + 1, tupleStr.length());
		Pattern sortp = Pattern.compile("\\.sort\\s*\\(\\s*(\\{.+\\})\\s*\\)");
		Matcher sortm = sortp.matcher(funs);
		if (sortm.find()) {
			sortbson = sortm.group(1);
			String prev = funs.substring(0, sortm.start());
			String end = funs.substring(sortm.end());
			funs = String.format("%s%s", prev, end);
		}

		String[] arr = funs.split("\\.");// 剩余简单函数，则按截取方式提取
		for (String fun : arr) {
			if (StringUtil.isEmpty(fun) || StringUtil.isEmpty(fun.trim())) {
				continue;
			}
			fun = fun.trim();
			if (fun.startsWith("count")) {
				hasCountFun = true;
			} else if (fun.startsWith("skip")) {
				String v = fun.substring(fun.indexOf("(") + 1,
						fun.lastIndexOf(")"));
				if (StringUtil.isEmpty(v)) {
					throw new EcmException("skip函数的参数必须。");
				}
				v = v.trim();
				skip = v;
			} else if (fun.startsWith("limit")) {
				String v = fun.substring(fun.indexOf("(") + 1,
						fun.lastIndexOf(")"));
				if (StringUtil.isEmpty(v)) {
					throw new EcmException("limit函数的参数必须。");
				}
				v = v.trim();
				limit = v;
			} else if (fun.startsWith("distinct")) {
				hasDistinctFun = true;
				// String v = fun.substring(fun.indexOf("(") + 1,
				// fun.lastIndexOf(")"));
				// if (StringUtil.isEmpty(v)) {
				// distinctClazz=String.class;
				// return;
				// }
				// v = v.trim();
				// try {
				// distinctClazz=(Class<?>)Class.forName(v);
				// } catch (ClassNotFoundException e) {
				// throw new
				// EcmException(String.format("distinct函数的参数类型%s不存在，原因：%s",v,
				// e));
				// }
			} else {
				throw new EcmException(String.format("mongodb不支持的函数:%s", fun));
			}
		}

	}

	@Override
	public long count() {
		if (!hasCountFun) {
			throw new EcmException("cubeql语句中未发现count()函数");
		}
		if(transcation!=null) {
			return countNotContainsPendding();
		}
		// String[] narr = fillParameters(tupleClazz).split("\\.");
		// String tupleColName = String.format("tuple_%s", narr[narr.length -
		// 1].trim());
		String tupleColName = fillParameters(this.tupleName);
		MongoCollection<Document> col = cube.cubedb.getCollection(tupleColName);
		CountOptions op = new CountOptions();

		if (!StringUtil.isEmpty(skip)) {
			String v = fillParameters(skip);
			int s = Integer.valueOf(v);
			op.skip(s);
		}
		if (!StringUtil.isEmpty(limit)) {
			String v = fillParameters(limit);
			int s = Integer.valueOf(v);
			op.limit(s);
		}
		// Document field = Document.parse(this.tupleFieldBson);//
		// 列一定非空，但它可能含有全部
		// boolean allField = false;
		// if (field.containsKey("tuple")) {
		// String v = (String) field.get("tuple");
		// if (!StringUtil.isEmpty(v) && v.trim().equals("*")) {
		// allField = true;
		// }
		// }
		// if (!allField) {
		// CJSystem.current().environment().logging()
		// .warn("统计函数语句中指定列没有意义，因此忽略");
		// }
		if (StringUtil.isEmpty(whereBson)) {
			Document filter = Document.parse("{}");
			return col.count(filter, op);
		}
		Document filter = Document.parse(fillParameters(whereBson));

		return col.count(filter, op);
	}

	private long countNotContainsPendding() {
		String tupleColName = fillParameters(this.tupleName);
		MongoCollection<Document> col = cube.cubedb.getCollection(tupleColName);
		CountOptions op = new CountOptions();

		if (!StringUtil.isEmpty(skip)) {
			String v = fillParameters(skip);
			int s = Integer.valueOf(v);
			op.skip(s);
		}
		if (!StringUtil.isEmpty(limit)) {
			String v = fillParameters(limit);
			int s = Integer.valueOf(v);
			op.limit(s);
		}
		
		if (StringUtil.isEmpty(whereBson)) {
			Document filter = Document.parse("{\"isPendding\":{\"$exists\":false}}");
			return col.count(filter, op);
		}
		//看看是否包含:号，因此查询条件一定是json一定会有:号，否则视为空的查询条件。有：号则isPendding之后加上,号，否则不加,号
		String wstr=whereBson;
		if(whereBson.indexOf(":")>-1) {
			wstr=wstr.replace("{", "{\"isPendding\":{\"$exists\":false},");
		}else {
			wstr=wstr.replace("{", "{\"isPendding\":{\"$exists\":false}");
		}
		Document filter = Document.parse(fillParameters(wstr));

		return col.count(filter, op);
	}
	@Override
	public IDocument<T> getSingleResult() {
		if (hasCountFun) {
			throw new EcmException(
					"getSingleResult方法不支持cubeql语句中的count()函数，请用：count方法");
		}
		// String[] narr = fillParameters(tupleClazz).split("\\.");
		// String tupleColName = String.format("tuple_%s", narr[narr.length -
		// 1].trim());
		String tupleColName = fillParameters(this.tupleName);
		MongoCollection<Document> col = cube.cubedb.getCollection(tupleColName);

		// Bson bson=new BasicDBObject();
		FindIterable<Document> find = null;
		Document field = Document.parse(this.tupleFieldBson);// 列一定非空，但它可能含有全部
		boolean allField = false;
		if (field.containsKey("tuple")) {
			String v = (String) field.get("tuple");
			if (!StringUtil.isEmpty(v) && v.trim().equals("*")) {
				allField = true;
			}
		}
		if (allField) {
			if (!StringUtil.isEmpty(whereBson)) {
				Document filter = Document.parse(fillParameters(whereBson));
				find = col.find(filter);
			} else {
				find = col.find();
			}
		} else {
			if (!StringUtil.isEmpty(whereBson)) {
				Document filter = Document.parse(fillParameters(whereBson));
				find = col.find(filter).projection(field);
			} else {
				find = col.find().projection(field);
			}
		}
		if (!StringUtil.isEmpty(skip)) {
			String v = fillParameters(skip);
			int s = Integer.valueOf(v);
			find.skip(s);
		}
		if (!StringUtil.isEmpty(limit)) {
			String v = fillParameters(limit);
			int s = Integer.valueOf(v);
			find.limit(s);
		}
		if (!StringUtil.isEmpty(sortbson)) {
			Document d = Document.parse(sortbson);
			find.sort(d);
		}
		Document doc = find.first();
		if (doc == null) {
			return null;
		}
		IDocument<T> tuple = convertDoc(doc);
		return tuple;
	}

	private void ifBaseFile(IDocument<T> tuple) {
		BaseFile file = (BaseFile) tuple.tuple();
		file.cube = cube;
		file.phyId = tuple.docid();
		file.coordinate = tuple.coordinate("system_fs_directories");
		if (file.otherCoords == null) {
			file.otherCoords = new HashMap<>();
		}
		if (tuple.existsCoordinate("createDate"))
			file.otherCoords.put("createDate", tuple.coordinate("createDate"));
		if (tuple.existsCoordinate("updateDate"))
			file.otherCoords.put("updateDate", tuple.coordinate("updateDate"));
		if (tuple.existsCoordinate("system_fs_files"))
			file.otherCoords.put("fileType",
					tuple.coordinate("system_fs_files"));
	}

	@Override
	public List<IDocument<T>> getResultList() {
		if (hasCountFun) {
			throw new EcmException("cubeql语句count()函数不能返回列表，请用：count方法");
		}
		String tclazz = fillParameters(tupleClazz);
		// String[] narr = tclazz.split("\\.");
		// String tupleColName = String.format("tuple_%s", narr[narr.length -
		// 1].trim());
		String tupleColName = fillParameters(this.tupleName);
		MongoCollection<Document> col = cube.cubedb.getCollection(tupleColName);// distinct
		Document filter = Document.parse(fillParameters(whereBson));
		FindIterable<Document> find = null;
		Document field = Document.parse(this.tupleFieldBson);// 列一定非空，但它可能含有全部
		boolean allField = false;
		if (field.containsKey("tuple")) {
			String v = (String) field.get("tuple");
			if (!StringUtil.isEmpty(v) && v.trim().equals("*")) {
				allField = true;
			}
		}
		if (hasDistinctFun) {
			return distinct(col, tclazz, filter, allField, field);
		}
		if (allField) {
			find = col.find(filter);
		} else {
			find = col.find(filter).projection(field);
		}
		if (!StringUtil.isEmpty(skip)) {
			String v = fillParameters(skip);
			int s = Integer.valueOf(v);
			find.skip(s);
		}
		if (!StringUtil.isEmpty(limit)) {
			String v = fillParameters(limit);
			int s = Integer.valueOf(v);
			find.limit(s);
		}
		if (!StringUtil.isEmpty(sortbson)) {
			Document d = Document.parse(sortbson);
			find.sort(d);
		}
		List<IDocument<T>> list = new ArrayList<>();
		MongoCursor<Document> it = find.iterator();
		while (it.hasNext()) {
			Document doc = it.next();
			IDocument<T> tuple = convertDoc(doc);
			if(this.transcation!=null&&!transcation.isContainsPenddingRows()) {
				continue;
			}
			list.add(tuple);
		}
		return list;
	}

	private List<IDocument<T>> distinct(MongoCollection<Document> col,
			String tupleclazz, Document filter, boolean allField,
			Document field) {
		String fieldName = "";
		Class<?> clazz = null;
		try {
			clazz = Class.forName(tupleclazz);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return new ArrayList<>();
		}
		if (allField) {
			fieldName = clazz.getDeclaredFields()[0].getName();
		} else {
			fieldName = field.keySet().toArray(new String[0])[0];
		}
		@SuppressWarnings("unchecked")
		DistinctIterable<T> di = (DistinctIterable<T>) col.distinct(fieldName,
				filter, clazz);
		MongoCursor<T> it = di.iterator();
		List<IDocument<T>> list = new ArrayList<>();
		while (it.hasNext()) {
			T o = it.next();
			TupleDocument<T> t = new TupleDocument<>();
			t.tulple = o;
			if(this.transcation!=null&&!transcation.isContainsPenddingRows()) {
				continue;
			}
			list.add(t);
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	private IDocument<T> convertDoc(Document doc) {
		try {

			TupleDocument<T> ret = new TupleDocument<T>();
			ret.isPendding=doc.getBoolean("isPendding", false);
			Set<String> set = doc.keySet();
			for (String key : set) {
				Object o = doc.get(key);
				if (!(o instanceof Document)) {
					if ("_id".equals(key)) {
						ObjectId id = (ObjectId) doc.get(key);
						ret.id = id.toString();
					}
					continue;
				}
				Document item = (Document) o;
				if ("tuple".equals(key)) {
					String tclazz = this.fillParameters(tupleClazz);

					Class<T> clazz = null;
					try {
						clazz = (Class<T>) Class.forName(tclazz.trim(),true,classloader);
					} catch (ClassNotFoundException e) {
						clazz = (Class<T>) Class.forName(tclazz.trim(), true,
								Thread.currentThread().getContextClassLoader());
					}
					// 只所以不用doc.toJson()是因为它对长整型会输出为对象，因此实现了MyJsonWriter并将之改为普通json类型
					MyJsonWriter writer = new MyJsonWriter(new StringWriter(),
							new JsonWriterSettings());
					new DocumentCodec().encode(writer, item,
							EncoderContext.builder()
									.isEncodingCollectibleDocument(true)
									.build());
					String json = writer.getWriter().toString();
					T tuple = new Gson().fromJson(json, clazz);
					ret.tulple = (T) tuple;
				} else {// 坐标
					Coordinate root = null;
					Coordinate prev = null;
					Set<String> props = item.keySet();
					for (String propName : props) {
						Object v = item.get(propName);
						Coordinate cur = new Coordinate(propName, v);
						if (prev == null) {
							root = cur;
							prev = cur;
							continue;
						}
						prev.nextLevel(cur);
						prev = cur;
					}
					if (root != null) {
						ret.addCoordinate(key, root);
					}

				}
			}
			if (ret.tulple instanceof BaseFile) {
				ifBaseFile(ret);
			}
			return ret;
		} catch (Exception e) {
			throw new EcmException(e);
		}
	}

	private String fillParameters(String exp) {
		Matcher m = this.paramPatt.matcher(exp);
		while (m.find()) {
			Object v = parameters.get(m.group(1));
			if (v == null) {
				throw new EcmException(String.format("缺少参数：%s", m.group(1)));
			}
			exp = exp.replace(exp.substring(m.start(), m.end()), v.toString());
			m = this.paramPatt.matcher(exp);
		}
		return exp;
	}

	@Override
	public void setParameter(String name, Object value) {
		this.parameters.put(name, value);

	}

	@Override
	public void removeParameter(String name) {
		this.parameters.remove(name);
	}
	
}
