package cj.lns.chip.sos.cube.framework;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;

import cj.lns.chip.sos.cube.framework.lock.FileLock;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.cube.framework.lock.ILock;
import cj.lns.chip.sos.cube.framework.lock.OpenShared;
import cj.studio.ecm.EcmException;
import cj.ultimate.util.StringUtil;

public class FileSystem {
	Cube cube;
	IChunkCollectionAssigner assigner;
	FileSystem(Cube cube) {
		this.cube = cube;
		assigner=new ChunkCollectionAssigner();
		assigner.init(cube);
	}

	/**
	 * 默认以OpenMode.openOrNew,OpenShared.clear参数打开文件
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param fn
	 * @return
	 * @throws FileNotFoundException
	 * @throws FileLockException
	 */
	public FileInfo openFile(String fn)
			throws FileNotFoundException, FileLockException {
		return openFile(fn, OpenMode.openOrNew, OpenShared.off);
	}

	public List<Coordinate> coordinateRoot(String dimName) {
		return cube.rootCoordinates("system_fs_files", dimName);
	}

	public List<Coordinate> coordinateChilds(String dimName,
			Coordinate coordinate) {
		return cube.childCoordinates("system_fs_files", dimName, coordinate);
	}
	/**
	 * 
	 * <pre>
	 *
	 * </pre>
	 * @param coordinates 注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @param lookAll
	 * @return
	 */
	public List<FileInfo> listFilesByCoordinate(Map<String, Coordinate> coordinates,boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<FileInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", FileInfo.class.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<FileInfo>> docs = q.getResultList();
		List<FileInfo> childs = new ArrayList<>();
		for (IDocument<FileInfo> doc : docs) {
			FileInfo file = doc.tuple();
			retrieveChunkCol(doc);
			if (file.lflag == 1) {// 文件使用了锁机制
				ILock lock = new FileLock(file.phyId,
						UUID.randomUUID().toString(),
						OpenShared.readwrite.getValue());
				file.init(cube, lock);
			} else {
				file.init(cube, null);
			}
			// file.phyId = doc.docid();

			childs.add(file);
		}
		return childs;
	}
	public long fileCountByCoordinate(Map<String, Coordinate> coordinates,boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<Long> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("replace", replace.toString());

		return q.count();
	}
	public long dirCountByCoordinate(Map<String, Coordinate> coordinates,boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count() "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':'$dir',?(replace)}";
		IQuery<Long> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("replace", replace.toString());
		return q.count();
	}
	/**
	 * 
	 * <pre>
	 *
	 * </pre>
	 * @param coordinates 注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @param lookAll
	 * @return
	 */
	public List<DirectoryInfo> listDirsByCoordinate(Map<String, Coordinate> coordinates,boolean lookAll) {
		Set<String> set = coordinates.keySet();
		StringBuffer replace = new StringBuffer();
		for (String dimName : set) {
			Coordinate coordinate = coordinates.get(dimName);
			cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':'$dir',?(replace)}";
		IQuery<DirectoryInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", DirectoryInfo.class.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<DirectoryInfo>> docs = q.getResultList();
		List<DirectoryInfo> childs = new ArrayList<>();
		for (IDocument<DirectoryInfo> doc : docs) {
			DirectoryInfo file = doc.tuple();
			file.cube=cube;
			file.coordinate=doc.coordinate("system_fs_directories");
			file.refresh();
			file.phyId = doc.docid();
			childs.add(file);
		}
		return childs;
	}
	/**
	 * 按坐标列出文件
	 * <pre>
	 *
	 * </pre>
	 * @param dimName
	 * @param coordinate 注意：参数中的坐标必须钻到实际元组的位置，如果都在坐标头，那查出来的只能是按坐标头的结果。
	 * @param lookAll true是每级均查出所有子级及孙级的文件，false是只列出本级直接包含的文件
	 * @return
	 */
	public List<FileInfo> listFilesByCoordinate(String dimName,
			Coordinate coordinate,boolean lookAll) {
		StringBuffer replace = new StringBuffer();
		cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<FileInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", FileInfo.class.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<FileInfo>> docs = q.getResultList();
		List<FileInfo> childs = new ArrayList<>();
		for (IDocument<FileInfo> doc : docs) {
			FileInfo file = doc.tuple();
			retrieveChunkCol(doc);
			if (file.lflag == 1) {// 文件使用了锁机制
				ILock lock = new FileLock(file.phyId,
						UUID.randomUUID().toString(),
						OpenShared.readwrite.getValue());
				file.init(cube, lock);
			} else {
				file.init(cube, null);
			}
			// file.phyId = doc.docid();

			childs.add(file);
		}
		return childs;
	}
	public long fileCountByCoordinate(String dimName,
			Coordinate coordinate,boolean lookAll) {
		StringBuffer replace = new StringBuffer();
		cube.combineWhereByCoordinate(dimName, coordinate, replace,lookAll);
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count() "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<Long> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("replace", replace.toString());
		return q.count();
	}
	public long dirsTotal() {
		String cql = "select {'tuple.name':1}.count() "
				+ " from tuple system_fs_files ?(tuple) "
				+ " where {\"system_fs_files.fileType\":'$dir'}";
		IQuery<Long> q = cube.createQuery(cql);
		q.setParameter("tuple", Long.class.getName());
		return q.count();
	}

	public List<String> enumFileType() {
		List<String> list = new ArrayList<String>();
		List<Coordinate> members = cube.rootCoordinates("system_fs_files",
				"system_fs_files");
		for (Coordinate coord : members) {
			if ("$dir".equals(coord.value()))
				continue;
			list.add((String) coord.value());
		}
		return list;
	}

	public List<FileInfo> listFilesByType(String fileType) {
		String cql = "select {'tuple':'*'} "
				+ " from tuple system_fs_files ?(tuple) "
				+ " where {\"system_fs_files.fileType\":'?(fileType)'}";
		IQuery<FileInfo> q = cube.createQuery(cql);
		q.setParameter("tuple", FileInfo.class.getName());
		q.setParameter("fileType", fileType);
		List<IDocument<FileInfo>> docs = q.getResultList();
		List<FileInfo> childs = new ArrayList<>();

		for (IDocument<FileInfo> doc : docs) {
			FileInfo file = doc.tuple();
			retrieveChunkCol(doc);
			if (file.lflag == 1) {// 文件使用了锁机制
				ILock lock = new FileLock(file.phyId,
						UUID.randomUUID().toString(),
						OpenShared.readwrite.getValue());
				file.init(cube, lock);
			} else {
				file.init(cube, null);
			}
			childs.add(file);
		}
		return childs;
	}

	public List<String> listFileNamesByType(String fileType) {
		List<String> list = new ArrayList<String>();
		String cql = "select {'tuple.name':1}.distinct() "
				+ " from tuple system_fs_files ?(tuple) "
				+ " where {\"system_fs_files.fileType\":'?(fileType)'}";
		IQuery<String> q = cube.createQuery(cql);
		q.setParameter("tuple", String.class.getName());
		q.setParameter("fileType", fileType);
		List<IDocument<String>> res = q.getResultList();
		for (IDocument<String> name : res) {
			list.add(name.tuple());
		}
		return list;
	}

	public long filesTotal() {
		List<Coordinate> members = cube.rootCoordinates("system_fs_files",
				"system_fs_files");
		long total = 0;
		for (Coordinate coord : members) {
			if ("$dir".equals(coord.value()))
				continue;
			String cql = "select {'tuple.name':1}.count() "
					+ " from tuple system_fs_files ?(tuple) "
					+ " where {\"system_fs_files.fileType\":'?(fileType)'}";
			IQuery<Long> q = cube.createQuery(cql);
			q.setParameter("tuple", Long.class.getName());
			q.setParameter("fileType", (String) coord.value());
			long c = q.count();
			total += c;
		}
		return total;
	}

	public double dataSize() {
		Document files = cube.tupleStats("system_fs_files");
		double j = convertToDouble(files.get("size"));
		Document chunks = cube.tupleStats("system_fs_chunks");
		double q = convertToDouble(chunks.get("size"));
		return j + q;
	}

	private double convertToDouble(Object o) {
		double j = 0;
		if (o instanceof Integer) {
			j = (int) o;
		} else if (o instanceof Double) {
			j = (double) o;
		} else {
			throw new EcmException("未实现类型");
		}
		return j;
	}

	public double usedSpace() {
		Document files = cube.tupleStats("system_fs_files");

		double i = convertToDouble(files.get("totalIndexSize"));
		double j = convertToDouble(files.get("storageSize"));
		Document chunks = cube.tupleStats("system_fs_chunks");
		double h = convertToDouble(chunks.get("totalIndexSize"));
		double q = convertToDouble(chunks.get("storageSize"));

		return 0D + i + j + h + q;
	}

	/**
	 * 默认以 OpenShared.clear参数打开文件
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param fn
	 * @param mode
	 * @return
	 * @throws FileNotFoundException
	 * @throws FileLockException
	 */
	public FileInfo openFile(String fn, OpenMode mode)
			throws FileNotFoundException, FileLockException {
		return openFile(fn, mode, OpenShared.off);
	}

	public FileInfo openFile(String fn, OpenMode mode, OpenShared shared)
			throws FileNotFoundException, FileLockException {
		String att[] = fn.split("/");
		String[] paths = new String[att.length - 1];
		System.arraycopy(att, 0, paths, 0, paths.length);

		String fileExt = "$unknown";
		String name = att[att.length - 1];
		if (name.contains(".")) {
			fileExt = name.substring(name.lastIndexOf(".") + 1, name.length());
		}
		boolean exsits = existsFile(paths, name);
		FileInfo file = null;
		switch (mode) {
		case createNew:
			if (exsits) {// 设空从头再写
				file = openFile(paths, name, fileExt, shared);
				file.setSpaceLength(0);
			} else {
				file = createFile(paths, name, fileExt, shared,null);
			}
			break;
		case onlyOpen:
			if (!exsits) {
				throw new FileNotFoundException(fn);
			}
			file = openFile(paths, name, fileExt, shared);
			break;
		case openOrNew:
			if (exsits) {// 如果存在则打开,否则新建
				file = openFile(paths, name, fileExt, shared);
			} else {
				file = createFile(paths, name, fileExt, shared,null);
			}
			break;
		}

		return file;
	}

	private boolean existsFile(String[] paths, String name) {
		StringBuffer replace = new StringBuffer();
		int i = 0;
		for (String p : paths) {
			if (StringUtil.isEmpty(p)) {
				continue;
			}
			replace.append(
					String.format("'system_fs_directories.%s':'%s',", i, p));
			i++;
		}
		// if (replace.length() < 1) {
		// replace.append("'system_fs_directories.-1':'/',");
		// }
		replace.append(String.format("'system_fs_directories.%s':{%s},", i,
				"$exists:false"));
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count() "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},'tuple.name':'?(name)','system_fs_directories.-1':'/',?(replace)}";
		IQuery<?> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("name", name);
		q.setParameter("replace", replace.toString());
		return q.count() > 0;
	}

	private FileInfo createFile(String[] paths, String name, String fileExt,
			OpenShared shared,Map<String ,Coordinate> others) throws FileNotFoundException {
		FileInfo file = new FileInfo();
		
		file.init(cube, null);
		file.name = name;
		file.createDate = Calendar.getInstance().getTime();
		TupleDocument<FileInfo> tuple = new TupleDocument<>(file);
		Coordinate dirs = new Coordinate("-1", "/");
		Coordinate tmp = dirs;
		int i = 0;
		for (String p : paths) {
			if (StringUtil.isEmpty(p))
				continue;
			tmp = tmp.nextLevel(new Coordinate(String.valueOf(i), p));
			i++;
		}
		DirectoryInfo dir = new DirectoryInfo();
		dir.cube = cube;
		dir.coordinate = dirs;
		if (!dir.exists()) {
			throw new FileNotFoundException("路径不存在：" + dir.path());
		}
		file.coordinate = dirs;
		tuple.addCoordinate("system_fs_directories", dirs);

		Coordinate filecoord = new Coordinate("fileType", fileExt);
		tuple.addCoordinate("system_fs_files", filecoord);
		
		MongoCollection<Document> chunkCol=assigner.assignChunkCol();
		file.chunk=chunkCol;
		String chunkColName=chunkCol.getNamespace().getCollectionName();
		Coordinate chunkcoord = new Coordinate("chunksColName", chunkColName);
		tuple.addCoordinate("system_fs_chunks", chunkcoord);
		
		if(others!=null&&!others.isEmpty()){//增加其它坐标
			Set<String> set=others.keySet();
			for(String dimName:set){
				tuple.addCoordinate(dimName, others.get(dimName));
			}
		}
		cube.saveDoc(null,"system_fs_files", tuple, true);
		file.phyId=tuple.docid();//由于在saveDoc时已为phyId赋值，因此不必再打开文件，故将下面代码注释。
		return file;
//		return openFile(paths, name, fileExt, shared);// 只所以再打开是因为要得到phyId
	}

	private FileInfo openFile(String[] paths, String name, String fileExt,
			OpenShared shared) {
		StringBuffer replace = new StringBuffer();
		int i = 0;
		for (String p : paths) {
			if (StringUtil.isEmpty(p)) {
				continue;
			}
			replace.append(
					String.format("'system_fs_directories.%s':'%s',", i, p));
			i++;
		}
		replace.append(String.format("'system_fs_directories.%s':{$exists:false},", i
				));
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'} "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},'tuple.name':'?(name)','system_fs_directories.-1':'/',?(replace)}";
		IQuery<FileInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", FileInfo.class.getName());
		q.setParameter("name", name);
		q.setParameter("replace", replace.toString());
		IDocument<FileInfo> tuple = q.getSingleResult();
		FileInfo bf = tuple.tuple();
		// bf.phyId = tuple.docid();
		retrieveChunkCol(tuple);
		if (OpenShared.off != shared) {// 如果是非共享则说明要使用锁，故置1
			bf.lflag = 1;
			updateFileLockFlag(bf);// 立即更新文件的锁开闭标志
			ILock lock = new FileLock(bf.phyId, UUID.randomUUID().toString(),
					shared.getValue());
			bf.init(cube, lock);
		} else {
			if (bf.lflag == 1) {
				ILock lock = new FileLock(bf.phyId,
						UUID.randomUUID().toString(), shared.getValue());
				bf.init(cube, lock);
			} else {
				bf.init(cube, null);
			}
		}
		// bf.coordinate = tuple.coordinate("system_fs_directories");

		return bf;
	}

	private MongoCollection<Document> retrieveChunkCol(
			IDocument<FileInfo> tuple) {
		Coordinate chunkcoord=tuple.coordinate("system_fs_chunks");
		if(chunkcoord==null){
			throw new EcmException(String.format("标识为：%s 的文件已损坏，原因：未知所在块集合", tuple.docid()));
		}
		String chunkColName=(String)chunkcoord.value();
		MongoCollection<Document> chunkCol=cube.cubedb.getCollection(chunkColName);
		tuple.tuple().chunk=chunkCol;
		return chunkCol;
	}

	private void updateFileLockFlag(FileInfo bf) {
		MongoCollection<Document> col = cube.cubedb
				.getCollection("system_fs_files");
		Document update = Document.parse("{'$set':{'tuple.lflag':1}}");
		col.updateOne(new BasicDBObject("_id", new ObjectId(bf.phyId)), update);
	}

	/**
	 * 获取目录信息，注意：该目录可能不存在
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param path
	 * @return
	 */
	public DirectoryInfo dir(String path) {
		return new DirectoryInfo(path, cube);
	}

	public boolean existsFile(String fn) {
		String att[] = fn.split("/");
		String[] paths = new String[att.length - 1];
		System.arraycopy(att, 0, paths, 0, paths.length);
		String name = att[att.length - 1];
		return existsFile(paths, name);
	}

	/**
	 * 为立方体初始化文件系统
	 * 
	 * <pre>
	 *
	 * </pre>
	 */
	public void init(CubeConfig conf) {
		Dimension dimdir = new Dimension("system_fs_directories");// 最多256个目录层级
		dimdir.alias = "目录视图";
		dimdir.desc = "按目录索引文件";
		Level root = new Level(
				new Property("-1", "根目录", String.class.getName()));
		dimdir.hierarcky().setHead(root);
		Level tmp = root;
		for (int i = 0; i < 255; i++) {
			tmp = tmp.nextLevel(new Level(new Property(String.valueOf(i),
					String.format("文件夹名", i), String.class.getName())));
		}
		cube.saveDimension(dimdir);
		Dimension dimfile = new Dimension("system_fs_files");
		dimfile.alias = "文件视图";
		dimfile.desc = "按文件类型索引文件";
		dimfile.hierarcky().setHead(new Level(
				new Property("fileType", "文件类型", String.class.getName())));// 其中$dir为目录类型
		cube.saveDimension(dimfile);

		DirectoryInfo dir = new DirectoryInfo("/", cube);
		dir.mkdir("根文件夹");
		
		//建立文件块集合维
		Dimension dimchunks = new Dimension("system_fs_chunks");
		dimchunks.alias = "文件块集合";
		dimchunks.desc = "用于定位文件所在的块集合";
		Level headchunks = new Level(
				new Property("chunksColName", "块集合名", String.class.getName()));
		dimchunks.hierarcky().setHead(headchunks);
		cube.saveDimension(dimchunks);
		
		assigner.init(cube);
	}

}
