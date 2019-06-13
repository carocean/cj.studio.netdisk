package cj.lns.chip.sos.cube.framework;

import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * 立方体
 * 
 * <pre>
 *
 * </pre>
 * 
 * @author carocean
 *
 */
public interface ICube {
	/**
	 * 
	 * <pre>
	 *
	 * </pre>
	 * @param binaryText 输入$binary字段的值，注意：不是$binary字段所在的mongodb文档对象的json串
	 * @return
	 */
	default byte[] decodeBinary(String binaryText){
		return DatatypeConverter.parseBase64Binary(binaryText);
	}
	default String encodeBinary(byte[] b){
		return DatatypeConverter.printBase64Binary(b);
	}
	Document cubeStats();
	Document tupleStats(String tupleName);
	Dimension dimension(String name);
	CubeConfig config();
	void updateCubeConfig(CubeConfig conf);
	double dataSize();
	double dataSize(String tupleName);
	double usedSpace();//已用空间大小
	double usedSpace(String tupleName);//已用空间大小
	List<String> enumDimension();
	
	boolean existsDimension(String name);

	void saveDimension(Dimension dim);

	void removeDimension(String name);

	List<Coordinate> rootCoordinates(String tupleName, String dimName);

	List<Coordinate> childCoordinates(String tupleName, String dimName,
			Coordinate coordinate);

	List<Coordinate> rootCoordinates(String dimName);

	List<Coordinate> childCoordinates(String dimName, Coordinate coordinate);

	boolean existsCoordinate(String dimName, String propName, Object member);

	void removeCoordinate(String dimName, String propName, Object member);

	void empty();

	List<Coordinate> tupleCoordinate();
	Dimension tupleDimension();
	void saveCoordinate(String dimName, Coordinate coord);

	boolean isEmpty();

	void deleteCube();

	/**
	 * 参见createquery方法
	 * 
	 * 支持的函数 skip(n),limit(n)
	 * 
	 * @param cubeql
	 * @return
	 */
	<T> IQuery<T> count(String cubeql);
	<T> IQuery<T> count(ITranscation tran,String cubeql);
	/**
	 * 使用cubejq查询语句
	 * 
	 * <pre>
	 * 语法：
	 * 	select x from tuple tupleName classFullName where y 返回列表
	 *	select x.count() from tuple tupleName classFullName where y    返回一个整数值
	 *	select x.skip(n).limit(n) from tuple tupleName classFullName where y 返回列表
	 *	select x.sort(z).skip(n).limit(n) from tuple tupleName classFullName where y 返回列表
	 *	如果没有查询条件，则使用： where {} 空的花括号
	 *
	 * 注：其中x,y,z均为bson对象，x是元组对象的bson，即花括号括起来{},x后面可跟函数。n为数值。
	 * tupleFullNamd是元组对象的类全名，如博客类cj.my.Blog，注意：短类名字：Blog用于查询，它对应的collection名字为：tuple_Blog,因此，注意短名不能与其它类重复，否则将视为同集合
	 * 
	 * 例：
	 * select {'tuple.name':1,'tuple.content':1}.count() from tuple blog cj.my.Blog where {'date.year':'2006','date.month':4,'creator.user':'zhao','location.country':'zh','location.city':'zhangzhou','tuple.name':'发布'}
	 *
	 * 支持的函数：
	 * skip(n),limit(n),sort(bson),distinct()
	 * 其中distinct函数的返回类型以tupleFullNamd指定，且必须是Java基本型，
	 * 它以select的字段为去重字段，以where为条件，因此，在使用去重函数时，select的字段只能指定一个，如果指定了多个或为全部，则默认使用第一个字段
	 * 例：
	 * select {'tuple.age':1}.distinct() " + " from tuple TestTupleEntity java.lang.Integer  where {"createDate.year":?(year)}
		如果要接收限定的字段列表的值，可使用HashMap类全名作为tupleClassName
	 * 其中：date,creator是维度名，.号之后是属性名，where语句中的tuple.name是将元组对象cj.my.Blog当维度使用
	 *  select {tuple:"*"} from ...
	 * </pre>
	 * 
	 * 注意： 泛型是要返回的tuple类型，该类型不一定必须是from tuple tupleName ClassName,它可以是任意类型
	 * 
	 * @param cubeql
	 * @return
	 */
	<T> IQuery<T> createQuery(String cubeql);
	<T> IQuery<T> createQuery(ITranscation tran,String cubeql);
	void close();
	/**
	 * 保存元组文档
	 * <pre>
	 * 如果元组对象中的字段不要持久，请用java 关键字transient声明字段，gson支持这个关键字,详情参见google Gson API
	 * </pre>
	 * @param tupleName
	 * @param doc
	 * @return 返回新产生的元组id
	 */
	String saveDoc(String tupleName, IDocument<?> doc);
	String saveDoc(ITranscation tran,String tupleName, IDocument<?> doc);
	UpdateResult updateDoc(String tupleName, String docid, IDocument<?> newdoc);
	UpdateResult updateDoc(ITranscation tran,String tupleName, String docid, IDocument<?> newdoc);
	UpdateResult updateDoc(String tupleName, String docid, IDocument<?> newdoc,UpdateOptions options);
	UpdateResult updateDoc(ITranscation tran,String tupleName, String docid, IDocument<?> newdoc,UpdateOptions options);
	void deleteDoc(String tupleName, IDocument<?> doc);
	void deleteDoc(String tupleName, String docid);
	void deleteDoc(ITranscation tran,String tupleName, String docid);
	long deleteDocs(String tupleName, String whereBson);
	FileSystem fileSystem();
	String name();
	/**
	 * 将给定的json串解析成坐标
	 * <pre>
	 * 格式，如:{'createDate':'2015/10/23','fileType':'doc','dir':'/我的文件/电影'}
	 * </pre>
	 * @param json
	 * @return
	 */
	Map<String, Coordinate> parseCoordinate(String json);
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
	<T>List<IDocument<T>> listTuplesByCoordinate(String tupleName,
			Class<T> tupleClazz, Map<String, Coordinate> coordinates,
			boolean lookAll);
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
	 *            注意要与维度级别属性定义的类型匹配，该方法根据级别属性定义的类型对坐标成员值进行了简单的转换，转换的类型有：java.lang.String,java.lang.Integer,java.lang.Long
	 * @param replace
	 * @param lookAll
	 *            每级是否包括全部子级及所有孙级成员的查询，false只查询本级的直接成员，如同目录只列出其直接包含的文件。
	 */
	void combineWhereByCoordinate(String dimName, Coordinate coordinate,
			StringBuffer replace, boolean lookAll);
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
	<T>List<IDocument<T>> listTuplesByCoordinate(String tupleName,
			Class<T> tupleClazz, String dimName, Coordinate coordinate,
			boolean lookAll);
	void importDims(String dimFile);
	void exportDims(String dimFile);
	void dropTuple(String tupleName);
	void importCoordinates(String coordFile);
	void exportCoordinates(String coordFile);
	void importOneDimByJson(String json);
	<T> AggregateIterable<T> aggregate(String tupleName,
			List<? extends Bson> pipeline, Class<T> resultClass);
	AggregateIterable<Document> aggregate(String tupleName,
			List<? extends Bson> pipeline);
	UpdateResult updateDocs(String tupleName,Bson filter, Bson update);
	long deleteDocOne(String tupleName, String whereBson);
	UpdateResult updateDocOne(String tupleName, Bson filter, Bson update);
	
	long listTuplesByCoordinate(String tupleName, String dimName,
			Coordinate coordinate, boolean lookAll);
	<T>List<IDocument<T>> listTuplesByCoordinate(String tupleName,
			Class<T> tupleClazz, String dimName, Coordinate coordinate,
			String sortBson, int skip, int limit, boolean lookAll);
	<T>List<IDocument<T>> listTuplesByCoordinate(String tupleName,
			Class<T> tupleClazz, Map<String, Coordinate> coordinates,
			String sortBson, int skip, int limit, boolean lookAll);
	UpdateResult updateDocOne(String tupleName, Bson filter, Bson update,
			UpdateOptions updateOptions);
	
	UpdateResult updateDocs(String tupleName, Bson filter, Bson update,
			UpdateOptions updateOptions);
	
	public <T> IDocument<T> document(String colname,String docid, Class<T> clazz) ;
	public <T> IDocument<T> document(ITranscation tran,String colname,String docid, Class<T> clazz) ;
	String createIndex(String tupleName, Bson keys);
	String createIndex(String tupleName, Bson keys, IndexOptions indexOptions);
	List<String> createIndexes(String tupleName, List<IndexModel> indexes);
	void dropIndex(String tupleName,String indexName);
	void dropIndex(String tupleName,Bson keys);
	ListIndexesIterable<Document> listIndexes(String tupleName);
	 <TResult> ListIndexesIterable<TResult> listIndexes(String tupleName,Class<TResult> resultClass);
	long tupleCount(String colname);
	long tupleCount(ITranscation tran,String colname);
	long tupleCount(String colname, String where);
	long tupleCount(ITranscation tran,String colname, String where);
	ITranscation begin();
	
}
