package cj.lns.chip.sos.cube.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.Cube;
import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.cube.framework.Dimension;
import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.Hierarcky;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.IReader;
import cj.lns.chip.sos.cube.framework.ITranscation;
import cj.lns.chip.sos.cube.framework.IWriter;
import cj.lns.chip.sos.cube.framework.Level;
import cj.lns.chip.sos.cube.framework.OpenMode;
import cj.lns.chip.sos.cube.framework.Property;
import cj.lns.chip.sos.cube.framework.TooLongException;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.disk.INetDisk;
import cj.lns.chip.sos.disk.NetDisk;

public class TestCube {
	@Test
	public void testExistsCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		System.out.println(Cube.exists(client, "zhaoxb"));
	}

	/*
	 * 主库分配空间配额，管理子库 子库知道属于哪个主库 这种主－子关系靠上层逻辑来维护，立方体功能本身不关心这些。 因此，主子关系是上层逻辑，不必在底层实现。
	 * 不必通过立方体名来确定主子关系了，那样不灵活。
	 */
	@Test
	public void testCreateCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		CubeConfig conf = new CubeConfig();
		conf.setDimFile(
				"/Users/jmal/git/diskcode/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
		conf.setCoordinateFile(
				"/Users/jmal/git/diskcode/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
		ICube cube = Cube.create(client, "cj", conf);
		System.out.println(cube);
	}

	@Test
	public void testOpenCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "jmal1", "lns", "lns");
		if (!disk.existsCube("test2")) {
			CubeConfig conf = new CubeConfig();
			disk.createCube("test2", conf);
			System.out.println("已创建test2存储方案");
		}
		ICube cube = disk.home();
		System.out.println(cube.name());
	}
	
	class User{
		String name;
		String age;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getAge() {
			return age;
		}
		public void setAge(String age) {
			this.age = age;
		}
	}
	
	ICube getCube(String host,String diskName) {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress(host));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, diskName, "lns", "lns");
		return disk.home();
	}

	@Test
	public void testCubeTransSave() {
		ICube cube = getCube("localhost", "lns.platform");
		ITranscation tran = cube.begin();
		try {
			User user = new User();
			user.setName("cj");
			user.setAge("1000");
			IDocument<?> doc = new TupleDocument<>(user);
			cube.saveDoc(tran, "test", doc);
			@SuppressWarnings("unused")
			int i = 1/0;
			ICube cube2 = getCube("localhost", "jmal1");
			user.setName("jmal2");
			user.setAge("20");
			IDocument<?> doc2 = new TupleDocument<>(user);
			cube2.saveDoc(tran, "test2", doc2);
			tran.commit();
		} catch (Exception e) {
			tran.rollback();
			throw e;
		}
		System.out.println(cube);
	}
	
	@Test
	public void testCubeTransUpdate() {
		ICube cube = getCube("localhost", "jmal1");
		ITranscation tran = cube.begin();
		User user = new User();
		try {
			String cql = "select {'tuple':'*'} from tuple test2 ?(tuple) where {'tuple.name':'jmal2'}";
			IQuery<User> q = cube.createQuery(cql);
			q.setParameter("tuple", User.class.getName());
			final IDocument<User> doc = q.getSingleResult();
			user = doc.tuple();
			user.setAge("777");
			cube.updateDoc(tran, "test2", doc.docid(), new TupleDocument<>(user));
			@SuppressWarnings("unused")
			int i = 1/0;	
			ICube cube2 = getCube("localhost", "lns.platform");
			String cql2 = "select {'tuple':'*'} from tuple test ?(tuple) where {'tuple.name':'jmal'}";
			IQuery<User> q2 = cube2.createQuery(cql2);
			q2.setParameter("tuple", User.class.getName());
			final IDocument<User> doc2 = q2.getSingleResult();
			user = doc2.tuple();
			user.setAge("1");
			cube2.updateDoc(tran, "test", doc2.docid(), new TupleDocument<>(user));
			tran.commit();
		} catch (Exception e) {
			tran.rollback();
			throw e;
		}
		System.out.println(cube);
	}
	
	@Test
	public void testCubeTransDelete() {
		ICube cube = getCube("localhost", "lns.platform");
		ITranscation tran = cube.begin();
		try {
			String cql = "select {'tuple':'*'} from tuple test ?(tuple) where {'tuple.name':'jmal'}";
			IQuery<User> q = cube.createQuery(cql);
			q.setParameter("tuple", User.class.getName());
			final IDocument<User> doc = q.getSingleResult();
			User user = doc.tuple();
			cube.deleteDoc(tran, "test", doc.docid());
			ICube cube2 = getCube("localhost", "jmal1");
			user.setName("jmal2");
			user.setAge("20");
			IDocument<?> doc2 = new TupleDocument<>(user);
			cube2.saveDoc(tran, "test2", doc2);
			user.setName("jmal2");
			user.setAge("20");
			IDocument<?> doc3 = new TupleDocument<>(user);
			cube.saveDoc(tran, "test2", doc3);
			user.setName("jmal2");
			user.setAge("20");
			IDocument<?> doc4 = new TupleDocument<>(user);
			cube2.saveDoc(tran, "test2", doc4);
			@SuppressWarnings("unused")
			int i = 1/0;
			tran.commit();
		} catch (Exception e) {
			tran.rollback();
			throw e;
		}
		System.out.println(cube);
	}

	@Test
	public void testDeleteCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		cube.deleteCube();
	}

	@Test
	public void testSetLength() throws IOException, FileLockException, TooLongException {
		// 该方法测试随机定位读写及空间长度
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		if (fs.existsFile("/fuck2.txt")) {
			FileInfo file = fs.openFile("/fuck2.txt", OpenMode.openOrNew);
			file.clearLocks();
			file.delete();
		}
		FileInfo file = fs.openFile("/fuck2.txt", OpenMode.openOrNew);
		file.clearLocks();
		file.setSpaceLength(15379456);// 末尾的偏移地址必是：15379455

		IWriter writer = file.writer(0);
		writer.write("-rw-------  1 carocean  wheel   128M  1  7 14:29 zhaoxb.1".getBytes());
		// writer.seek(-1);//如果空间长度是15379456，定位到它的末尾seek=-1位便是15379455，之后在读写时指定下移至15379456开始，因此不会影响15379455位置原来的数据，故为追加模式
		// for(int i=0;i<1000;i++)
		// writer.write("abcdefghijklmnopqrstuvwsyz".getBytes());
		writer.close();
		//
		FileOutputStream out = new FileOutputStream("/Users/carocean/Downloads/fuck2.txt");
		int read = 0;
		byte[] b = new byte[10 * 1024];
		IReader reader = file.reader(0);
		while ((read = reader.read(b, 0, b.length)) > -1) {
			out.write(b, 0, read);
		}
		// reader.seek(10000);
		// read=reader.read(b);
		// byte[] data=new byte[read];
		// System.arraycopy(b, 0, data, 0, read);
		// System.out.println(new String(reader.readFully()));
		reader.close();
		out.close();
		System.out.println(file.spaceLength() + " " + file.dataLength());

	}

	@Test
	public void testReadLns() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo root = fs.dir("/");
		String dir = "/Users/carocean/Downloads/temp";
		writeDir(dir, root);
		System.out.println("读完");
	}

	private void writeDir(String d, DirectoryInfo parent) throws FileLockException, IOException {
		for (DirectoryInfo dir : parent.listDirs()) {
			System.out.println(dir.path() + " " + dir.name());
			List<FileInfo> files = dir.listFiles();
			for (FileInfo file : files) {
				String fn = String.format("%s/%s/%s", d, dir.path(), file.name());
				System.out.println(file.fullName());
				File f = new File(fn);
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}
				FileOutputStream out = new FileOutputStream(f);
				int read = 0;
				byte[] b = new byte[10 * 1024];
				IReader reader = file.reader(0);
				while ((read = reader.read(b, 0, b.length)) > -1) {
					out.write(b, 0, read);
				}
				reader.close();
				out.close();
			}
			writeDir(d, dir);
		}
	}

	@Test
	public void testWriteLns() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		writeLns(fs, new File("/Users/carocean/studio/lns"));
	}

	int writeFileCount = 0;

	private void writeLns(FileSystem fs, File file) throws FileLockException, IOException {
		// if (writeFileCount > 1000)
		// return;
		String name = file.getAbsolutePath();
		String lname = name.substring("/Users/carocean/studio/lns".length(), name.length());
		if (lname.startsWith("/.") || lname.startsWith("/build"))
			return;
		if (lname.endsWith(".class"))
			return;
		if (file.isDirectory()) {
			fs.dir(lname).mkdir(file.getName());
			;
			File[] files = file.listFiles();
			for (File f : files) {
				writeLns(fs, f);
			}
		} else {

			FileInfo fi = fs.openFile(lname);
			FileInputStream in = new FileInputStream(file);
			// FileInfo fi=fs.openFile("/my/simpleData.html");
			// FileInputStream in =new
			// FileInputStream("/Users/carocean/Downloads/simpleData.html");
			int read = 0;
			byte[] b = new byte[12 * 1024];
			int total = 0;
			fi.clearLocks();
			IWriter writer = fi.writer(0);
			while ((read = in.read(b, 0, b.length)) > -1) {
				writer.write(b, 0, read);
				total += read;
			}
			System.out.println(lname + " " + total);
			in.close();
			writer.close();
			writeFileCount++;
		}
	}

	@Test
	public void testWriteFile() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		// FileInfo fi=fs.openFile("/my/IMG_2517.JPG");
		// FileInputStream in =new
		// FileInputStream("/Users/carocean/Downloads/IMG_2517.JPG");
		FileInfo fi = fs.openFile("/my/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		FileInputStream in = new FileInputStream(
				"/Users/carocean/Downloads/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		// FileInfo fi=fs.openFile("/my/simpleData.html");
		// FileInputStream in =new
		// FileInputStream("/Users/carocean/Downloads/simpleData.html");
		int read = 0;
		byte[] b = new byte[12 * 1024];
		int total = 0;
		fi.clearLocks();
		IWriter writer = fi.writer(0);
		while ((read = in.read(b, 0, b.length)) > -1) {
			writer.write(b, 0, read);
			total += read;
			// System.out.println(String.format("已读：%s,已写：%s",total,fi.length));
		}
		System.out.println(total);
		in.close();
		writer.close();
	}

	@Test
	public void testDirectoryRefresh() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("121.201.67.55"));
		List<MongoCredential> credentials = new ArrayList<>();
		MongoCredential c = MongoCredential.createCredential("caroceanjofers", "test", "!jofers276017".toCharArray());
		credentials.add(c);
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credentials, options);
		ICube cube = NetDisk.trustOpen(client, "natural").cube("6646608553");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo my = fs.dir("/products");
		my.refresh();
		System.out.println(my.name());
	}

	@Test
	public void testDirectoryRename() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo my = fs.dir("/my");
		my.renameFolderName("我的文件夹3");
		System.out.println(my.exists());
	}

	@Test
	public void testFileSystemUsedSpace() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		System.out.println("立方体配额：");
		System.out.println(cube.config().getCapacity() / 1024 / 1024 / 1024 + "G");
		System.out.println("立方体已用空间：");
		System.out.println(cube.usedSpace() / 1024 / 1024 + "M " + cube.usedSpace());
		System.out.println("立方体数据用量：");
		System.out.println(cube.dataSize() / 1024 / 1024 + "M " + cube.dataSize());
		System.out.println("文件系统空间占用：");
		System.out.println(fs.usedSpace() / 1024 / 1024 + "M " + fs.usedSpace());
		System.out.println("文件系统数据用量：");
		System.out.println(fs.dataSize() / 1024 / 1024 + "M " + fs.dataSize());
	}

	@Test
	public void testDirectoryDelete() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo my = fs.dir("/my");
		my.delete();
		System.out.println(my.exists());
	}

	@Test
	public void testDirectoryMkdirs() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo lns = fs.dir("/my/studio/lns");
		lns.mkdir("lns代码库");
		System.out.println(lns.exists());
	}

	@Test
	public void testDirectoryMkdir() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo lns = fs.dir("/my/studio/lns");
		lns.mkdir("lns代码库");

		DirectoryInfo my = fs.dir("/my");
		my.mkdir("我的文件夹");
		DirectoryInfo downloads = fs.dir("/downloads");
		downloads.mkdir("下载");
		DirectoryInfo desktop = fs.dir("/desktop");
		desktop.mkdir("桌面");
		DirectoryInfo test1 = fs.dir("/my/test1");
		test1.mkdir("测试1");
		DirectoryInfo test2 = fs.dir("/my/test2");
		test2.mkdir("测试2");
		System.out.println(my.exists());
	}

	@Test
	public void testDirectoryExists() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo root = fs.dir("/");

		System.out.println(root.exists());
	}

	@Test
	public void testListDirectory() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo root = fs.dir("/");

		printDir("--", root);

	}

	private void printDir(String indent, DirectoryInfo parent) {
		for (DirectoryInfo dir : parent.listDirs()) {
			System.out.println(dir.path() + " " + dir.name());
			List<FileInfo> files = dir.listFiles();
			for (FileInfo file : files) {
				System.out.println(indent + file.name() + " " + file.spaceLength());
			}
			printDir(indent, dir);
		}
	}

	@Test
	public void testDeleteFile() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		// FileInfo fi=fs.openFile("/my/IMG_2517.JPG");
		// FileOutputStream out =new
		// FileOutputStream("/Users/carocean/Downloads/IMG_25172.JPG");
		FileInfo fi = fs.openFile("/my/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		fi.delete();

	}

	@Test
	public void testReadFullyFile() throws IOException, FileLockException, TooLongException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		FileInfo fi = fs.openFile("/my/simpleData.html");
		FileOutputStream out = new FileOutputStream("/Users/carocean/Downloads/simpleData2.html");
		// FileInfo
		// fi=fs.openFile("/my/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		// FileOutputStream out =new
		// FileOutputStream("/Users/carocean/Downloads/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字]3.mkv");
		fi.clearLocks();
		// fi.setLength(10000100);
		int total = 0;
		IReader reader = fi.reader(0);
		byte[] b = reader.readFully();
		out.write(b);
		System.out.println(new String(b));
		reader.close();
		out.close();
		System.out.println(total + "......." + fi.dataLength() + " " + fi.spaceLength());
	}

	@Test
	public void testReadFile() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		// FileInfo fi=fs.openFile("/my/IMG_2517.JPG");
		// FileOutputStream out =new
		// FileOutputStream("/Users/carocean/Downloads/IMG_25172.JPG");
		FileInfo fi = fs.openFile("/my/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		FileOutputStream out = new FileOutputStream(
				"/Users/carocean/Downloads/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字]3.mkv");
		fi.clearLocks();
		// fi.setLength(10000100);
		int read = 0;
		int total = 0;
		byte[] b = new byte[10 * 1024];
		IReader reader = fi.reader(0);
		reader.seek(274060000);
		while ((read = reader.read(b, 0, b.length)) > -1) {
			out.write(b, 0, read);
			total += read;
		}
		reader.close();
		out.close();
		System.out.println(total + "......." + fi.dataLength() + " " + fi.spaceLength());
	}

	@Test
	public void testSaveTuple() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		for (int i = 0; i < 10000; i++) {
			TestTupleEntity tuple = new TestTupleEntity();
			tuple.entity2 = new TestEntity2();
			tuple.entity2.ssss = "dddddd_" + i;
			tuple.name = "fuck_" + i;
			tuple.sss = "dkdkddkd";
			tuple.ttt = 222 + i;
			IDocument<TestTupleEntity> doc = new TupleDocument<TestTupleEntity>(tuple);
			cube.saveDoc("tuple_TestTupleEntity", doc);
		}
	}

	@Test
	public void testQueryTuple() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple':'*'} " + " from tuple ?(tupleName) ?(tupleClassName) "
				+ " where {\"createDate.year\":?(year),\"tuple.name\":\"?(name)\"}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tupleName", "tuple_TestTupleEntity");
		q.setParameter("tupleClassName", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		q.setParameter("name", "fuck_5000");

		List<IDocument<TestTupleEntity>> docs = q.getResultList();
		for (IDocument<TestTupleEntity> doc : docs) {
			System.out.println(doc.tuple());
		}
	}

	@Test
	public void testQueryTupleByFields() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple.name':1} " + " from tuple tuple_TestTupleEntity ?(tuple) "
				+ " where {\"createDate.year\":?(year),\"tuple.name\":\"?(name)\"}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		q.setParameter("name", "fuck_5000");

		List<IDocument<TestTupleEntity>> docs = q.getResultList();
		for (IDocument<TestTupleEntity> doc : docs) {
			System.out.println(doc.tuple());
		}
	}

	@Test
	public void testQueryTupleByLimit() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple.name':1}.skip(?(skip)).limit(?(limit)) "
				+ " from tuple tuple_TestTupleEntity ?(tuple) " + " where {\"createDate.year\":?(year)}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		q.setParameter("skip", 20);
		q.setParameter("limit", 100);
		List<IDocument<TestTupleEntity>> docs = q.getResultList();
		for (IDocument<TestTupleEntity> doc : docs) {
			TestTupleEntity en = (TestTupleEntity) doc.tuple();
			System.out.println(en.name);
		}
	}

	@Test
	public void testGetConfig() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		System.out.println(cube.config());
	}

	@Test
	public void testUpdateConfig() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		CubeConfig conf = cube.config();
		System.out.println(conf.getCapacity());
		conf.setCapacity(4 * 1024 * 1024 * 1024L);
		cube.updateCubeConfig(conf);
		conf = cube.config();
		System.out.println(conf.getCapacity());
	}

	@Test
	public void testTupleUsedSpace() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		System.out.println(cube.usedSpace("tuple_TestTupleEntity"));
	}

	@Test
	public void testCubeUsedSpace() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		System.out.println(cube.usedSpace());
	}

	@Test
	public void testQueryTupleFindOne() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple.name':1}.skip(10).limit(3) " + " from     tuple tuple_TestTupleEntity   ?(tuple) "
				+ " where {\"createDate.year\":?(year)}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		IDocument<TestTupleEntity> doc = q.getSingleResult();
		TestTupleEntity en = (TestTupleEntity) doc.tuple();
		System.out.println(en.name);
	}

	@Test
	public void testUpdateTuple() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple':'*'} " + " from     tuple tuple_TestTupleEntity   ?(tuple) "
				+ " where {\"_id\":?(id)}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("id", "ObjectId('5686a4cee9c980b0a082b79a')");
		IDocument<TestTupleEntity> doc = q.getSingleResult();
		TestTupleEntity en = (TestTupleEntity) doc.tuple();
		System.out.println(en.name);
		TestTupleEntity te = new TestTupleEntity();
		te.name = "update38388";
		te.sss = "更新23333";
		te.ttt = 1000232322;
		te.entity2 = new TestEntity2();
		te.entity2.ssss = "这是更新的明细2344";
		IDocument<TestTupleEntity> newdoc = new TupleDocument<>(te);
		cube.updateDoc("tuple_TestTupleEntity", doc.docid(), newdoc);

		// new GridFS(null).
	}

	@Test
	public void testQueryTupleByLimitCount() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple':'*'}.count() " + " from tuple tuple_TestTupleEntity ?(tuple) "
				+ " where {\"createDate.year\":?(year)}";
		IQuery<TestTupleEntity> q = cube.count(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		System.out.println(q.count());
	}

	// distinct
	@Test
	public void testQueryTupleByDistinct() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple.sss':1}.distinct() " + " from tuple tuple_TestTupleEntity ?(tuple) "
				+ " where {\"createDate.year\":?(year)}";
		IQuery<String> q = cube.createQuery(cql);

		q.setParameter("tuple", String.class.getName());
		q.setParameter("year", 2016);
		List<IDocument<String>> docs = q.getResultList();
		for (IDocument<String> doc : docs) {
			System.out.println(doc.tuple());
		}
	}

	@Test
	public void testQueryTupleBySort() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple.name':1}.skip(10).limit(10).sort({'tuple.name':-1}) "
				+ " from tuple tuple_TestTupleEntity ?(tuple) " + " where {\"createDate.year\":?(year)}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		List<IDocument<TestTupleEntity>> docs = q.getResultList();
		for (IDocument<TestTupleEntity> doc : docs) {
			TestTupleEntity en = (TestTupleEntity) doc.tuple();
			System.out.println(en.name);
		}
	}

	@Test
	public void testFindFileSystemFileTypes() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		List<Coordinate> members = cube.rootCoordinates("system_fs_files", "system_fs_files");
		Map<String, Long> values = new HashMap<>();
		long total = 0;
		for (Coordinate coord : members) {
			if ("$dir".equals(coord.value()))
				continue;

			String cql = "select {'tuple.name':1}.count() " + " from tuple system_fs_files ?(tuple) "
					+ " where {\"system_fs_files.fileType\":'?(fileType)'}";
			IQuery<TestTupleEntity> q = cube.createQuery(cql);
			q.setParameter("tuple", Long.class.getName());
			q.setParameter("fileType", (String) coord.value());
			long c = q.count();
			values.put((String) coord.value(), c);
			total += c;

		}

		Set<String> set = values.keySet();
		for (String k : set) {
			Long l = values.get(k);
			System.out.println(k + " " + l);
		}
		System.out.println("总文件数：" + total);
		String cql = "select {'tuple.name':1}.count() " + " from tuple system_fs_files ?(tuple) "
				+ " where {\"system_fs_files.fileType\":'$dir'}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tuple", Long.class.getName());
		long c = q.count();
		System.out.println("文件夹有：" + c);

		// cql = "select {'tuple':'*'} "
		// + " from tuple system_fs_files ?(tuple) "
		// + " where {\"system_fs_files.fileType\":'$dir'}";
		// IQuery<DirectoryInfo> qdir = cube.createQuery(cql);
		// qdir.setParameter("tuple", DirectoryInfo.class.getName());
		// List<IDocument<DirectoryInfo>> docs=qdir.getResultList();
		// for(IDocument<DirectoryInfo> doc:docs){
		// System.out.println(doc.tuple().fileCount()+" "+doc.tuple().path());
		// }
	}

	@Test
	public void testFindTupleCoordinates() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		List<Coordinate> members = cube.rootCoordinates("tuple_TestTupleEntity", "createDate");
		for (Coordinate coord : members) {
			System.out.println(coord.value());
			List<Coordinate> childs = cube.childCoordinates("tuple_TestTupleEntity", "createDate", coord);
			for (Coordinate c : childs) {
				System.out.println(" " + c.value());
				List<Coordinate> day = cube.childCoordinates("tuple_TestTupleEntity", "createDate", c);
				for (Coordinate d : day) {
					System.out.println("  " + d.value());
				}
			}

		}
	}

	@Test
	public void testListDims() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		List<String> list = cube.enumDimension();
		for (String d : list) {
			System.out.println(d);
		}
	}

	@Test
	public void testDeleteDim() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		cube.removeDimension("locate");
	}

	@Test
	public void testSaveDim() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		Dimension dim = new Dimension();
		dim.setName("locate");
		dim.setAlias("地区");
		dim.setDesc("地理位置");
		Hierarcky hier = dim.hierarcky();
		Level country = new Level(new Property("country", "国家", "java.lang.String"));
		country.nextLevel(new Level(new Property("city", "城市", "java.lang.String")))
				.nextLevel(new Level(new Property("region", "区", "java.lang.String")));
		hier.setHead(country);

		cube.saveDimension(dim);
	}

	@Test
	public void testSaveCoord() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");

		Coordinate coord = new Coordinate("year", 2015);
		coord.nextLevel(new Coordinate("month", 5)).nextLevel(new Coordinate("day", 30));
		cube.saveCoordinate("createDate", coord);
	}

	@Test
	public void testDeleteCoord() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");

		cube.removeCoordinate("createDate", "day", 30);
	}

	@Test
	public void testFindDefCoordinates() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		List<Coordinate> members = cube.rootCoordinates("createDate");
		for (Coordinate coord : members) {
			System.out.println(coord.value());
			List<Coordinate> childs = cube.childCoordinates("createDate", coord);
			for (Coordinate c : childs) {
				System.out.println(" " + c.value());
				List<Coordinate> day = cube.childCoordinates("createDate", c);
				for (Coordinate d : day) {
					System.out.println("  " + d.value());
				}
			}

		}
	}

	@Test
	public void testEmptyCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		cube.empty();
	}

	@Test
	public void testDeleteTuple() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		String cql = "select {'tuple':'*'} " + " from tuple ?(tupleName) ?(tupleClass) "
				+ " where {\"createDate.year\":?(year)}";
		IQuery<TestTupleEntity> q = cube.createQuery(cql);
		q.setParameter("tupleName", "tuple_TestTupleEntity");
		q.setParameter("tupleClass", TestTupleEntity.class.getName());
		q.setParameter("year", 2016);
		// q.setParameter("name", "fuck_5000");

		List<IDocument<TestTupleEntity>> docs = q.getResultList();
		for (IDocument<?> doc : docs) {
			cube.deleteDoc("tuple_TestTupleEntity", doc);
		}
	}

}
