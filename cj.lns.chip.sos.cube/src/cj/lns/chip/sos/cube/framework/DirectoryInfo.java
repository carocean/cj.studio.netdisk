package cj.lns.chip.sos.cube.framework;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import cj.lns.chip.sos.cube.framework.lock.FileLock;
import cj.lns.chip.sos.cube.framework.lock.ILock;
import cj.lns.chip.sos.cube.framework.lock.OpenShared;
import cj.studio.ecm.EcmException;
import cj.ultimate.util.StringUtil;

/**
 * 目录对象具有路径和目录名（末路径），必是一个文件夹，因此可为文件夹起名。
 * 
 * <pre>
 *
 * </pre>
 * 
 * @author carocean
 *
 */
public class DirectoryInfo extends BaseFile {

	DirectoryInfo() {
		// TODO Auto-generated constructor stub
	}

	public String path() {
		String path = "";
		Coordinate tmp = coordinate;
		while (tmp != null) {
			path += String.format("%s/",
					"-1".equals(tmp.propName()) ? "" : tmp.value());
			tmp = tmp.nextLevel();
		}
		return path;
	}

	public String dirName() {
		Coordinate last = coordinate.goLast();
		return (String) last.value();
	}

	public DirectoryInfo(Coordinate coordinate, Cube cube) {
		this.coordinate = coordinate;
		this.cube = cube;
	}

	public DirectoryInfo(String path, Cube cube) {
		this.cube = cube;
		if ("/".equals(path)) {
			coordinate = new Coordinate("-1", path);
		} else {
			if (!path.startsWith("/")) {
				throw new EcmException("路径须以/开头");
			}
			String[] paths = path.replace("\\", "/").replace("//", "/")
					.split("/");
			coordinate = new Coordinate("-1", "/");
			Coordinate tmp = coordinate;
			int i = 0;
			for (String p : paths) {
				if (StringUtil.isEmpty(p))
					continue;
				tmp = tmp.nextLevel(new Coordinate(String.valueOf(i), p));
				i++;
			}
		}
	}

	public DirectoryInfo parent() {
		Coordinate coord = coordinate.goLast();
		if (coord.parentLevel == null)
			return null;
		DirectoryInfo dir = new DirectoryInfo(
				coord.parentLevel.truncateAndGoHead(), cube);
		dir.refresh();
		return dir;
	}

	public long fileCount() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		// 求当前目录的文件的算法：下个目录不存在
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 1), "$exists:false"));
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple.name':1}.count() "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<HashMap<String, String>> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", HashMap.class.getName());
		q.setParameter("replace", replace.toString());
		return q.count();
	}

	/**
	 * 刷新目录
	 * 
	 * <pre>
	 *
	 * </pre>
	 */
	public void refresh() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			tmp = tmp.nextLevel();
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.limit(1) "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':'$dir','system_fs_directories.-1':'/',?(replace)}";
		IQuery<DirectoryInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", DirectoryInfo.class.getName());
		q.setParameter("replace", replace.toString());
		IDocument<DirectoryInfo> tuple = q.getSingleResult();
		if (tuple == null)
			return;
		DirectoryInfo dir = tuple.tuple();
		phyId = tuple.docid();
		attrs = dir.attrs;
		createDate = dir.createDate;
		name = dir.name;
	}

	public List<String> listFileNames() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		// 求当前目录的文件的算法：下个目录不存在
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 1), "$exists:false"));
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple.name':1} "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':{$ne:'$dir'},?(replace)}";
		IQuery<HashMap<String, String>> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", HashMap.class.getName());
		q.setParameter("replace", replace.toString());

		List<IDocument<HashMap<String, String>>> docs = q.getResultList();
		List<String> childs = new ArrayList<>();
		for (IDocument<HashMap<String, String>> doc : docs) {
			String dir = doc.tuple().get("name");
			childs.add(dir);
		}
		return childs;
	}

	public List<FileInfo> listFiles() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		// 求当前目录的文件的算法：下个目录不存在
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 1), "$exists:false"));
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
//			file.phyId = doc.docid();
			
			childs.add(file);
		}
		return childs;
	}
	private MongoCollection<Document> retrieveChunkCol(
			IDocument<FileInfo> tuple) {
		Coordinate chunkcoord=tuple.coordinate("system_fs_chunks");
		String chunkColName=(String)chunkcoord.value();
		MongoCollection<Document> chunkCol=cube.cubedb.getCollection(chunkColName);
		tuple.tuple().chunk=chunkCol;
		return chunkCol;
	}
	/**
	 * 列出子目录
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @return
	 */
	public List<DirectoryInfo> listDirs() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		// 求子目录算法：一下个目录存在且下下个不存在
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 1), "$exists:true"));
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 2), "$exists:false"));
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
			DirectoryInfo dir = doc.tuple();
			dir.cube = cube;
			dir.coordinate = doc.coordinate("system_fs_directories");
			childs.add(dir);
		}
		return childs;
	}

	/**
	 * 删除当前目录及其下所有子目录与文件
	 * 
	 * <pre>
	 *
	 * </pre>
	 */
	public void delete() {
		this.deleteFiles(this);
		deleteDirs(this);
	}

	private void deleteDirs(DirectoryInfo dir) {
		String whereBson = "{'system_fs_files.fileType':'$dir',%s}";
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = dir.coordinate;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			tmp = tmp.nextLevel();
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		whereBson = String.format(whereBson, replace.toString());
		cube.deleteDocs("system_fs_files", whereBson);
	}

	private void deleteFiles(DirectoryInfo dir) {
		List<FileInfo> files = dir.listFiles();
		for (FileInfo f : files) {
			f.delete();
		}
		List<DirectoryInfo> dirs = dir.listDirs();
		for (DirectoryInfo d : dirs) {
			dir.deleteFiles(d);
		}
	}

	public boolean exists() {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		Coordinate last = null;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			if (tmp.nextLevel() == null) {
				last = tmp;
			}
			tmp = tmp.nextLevel();
		}
		// 求当前目录且无子目录算法：下一个不存在
		replace.append(String.format("'system_fs_directories.%s':{%s},",
				(Integer.valueOf(last.propName()) + 1), "$exists:false"));
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.count() "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':'$dir','system_fs_directories.-1':'/',?(replace)}";
		IQuery<DirectoryInfo> q = cube.count(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", Long.class.getName());
		q.setParameter("replace", replace.toString());
		return q.count() > 0;
	}

	public void renameFolderName(String folderName) {
		StringBuffer replace = new StringBuffer();
		Coordinate tmp = coordinate;
		while (tmp != null) {
			replace.append(String.format("'system_fs_directories.%s':'%s',",
					tmp.propName(), tmp.value()));
			tmp = tmp.nextLevel();
		}
		if (replace.charAt(replace.length() - 1) == ',') {
			replace.deleteCharAt(replace.length() - 1);
		}
		String cql = "select {'tuple':'*'}.limit(1) "
				+ " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {'system_fs_files.fileType':'$dir','system_fs_directories.-1':'/',?(replace)}";
		IQuery<DirectoryInfo> q = cube.createQuery(cql);
		q.setParameter("tupleName", "system_fs_files");
		q.setParameter("tupleClassName", DirectoryInfo.class.getName());
		q.setParameter("replace", replace.toString());
		IDocument<DirectoryInfo> dir = q.getSingleResult();
		if (dir == null)
			return;
		if (StringUtil.isEmpty(phyId)) {
			phyId = dir.docid();
			attrs = dir.tuple().attrs;
			createDate = dir.tuple().createDate;
		}
		dir.tuple().attrs = this.attrs;
		dir.tuple().name = folderName;
		cube.updateDoc("system_fs_files", dir.docid(), dir);
		name = folderName;
	}

	public void mkdir(String folderName) {
		// 检查每一节目录，如果不存在则创建
		Coordinate tmp = this.coordinate;
		while (tmp != null) {

			if (dirName().equals(tmp.value())) {
				this.name = folderName;
				this.mkdirThis(this);
			} else {
				Coordinate coord = tmp.truncateAndGoHead();
				DirectoryInfo dir = new DirectoryInfo(coord, cube);
				dir.mkdirThis(dir);
			}

			tmp = tmp.nextLevel();
		}
	}

	private void mkdirThis(DirectoryInfo dir) {

		if (dir.exists()) {
			return;
		}

		dir.createDate = Calendar.getInstance().getTime();

		IDocument<DirectoryInfo> tupleDir = new TupleDocument<DirectoryInfo>(
				dir);
		Coordinate dircoord = dir.coordinate;
		if (StringUtil.isEmpty(dir.name)) {
			dir.name = dir.dirName();
		}
		Coordinate filecoord = new Coordinate("fileType", "$dir");
		tupleDir.addCoordinate("system_fs_files", filecoord);
		tupleDir.addCoordinate("system_fs_directories", dircoord);
		cube.saveDoc(null,"system_fs_files", tupleDir, true);
	}

}
