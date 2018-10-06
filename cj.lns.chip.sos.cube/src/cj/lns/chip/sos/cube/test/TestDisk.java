package cj.lns.chip.sos.cube.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IReader;
import cj.lns.chip.sos.cube.framework.IWriter;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.disk.DiskInfo;
import cj.lns.chip.sos.disk.INetDisk;
import cj.lns.chip.sos.disk.NetDisk;

public class TestDisk {

	@SuppressWarnings("unused")
	@Test
	public void testNetDiskScript() {
		MongoClient client = null;
		String name = "zhaoxb";
		String password = "11";
		String userName = "2-3-3-2";
		INetDisk disk = NetDisk.create(client, name, userName, password, null);
		disk = NetDisk.open(client, name, userName, password);
		List<String> diskNames = NetDisk.enumDisk(client);
		if (NetDisk.existsDisk(client, name)) {

		}
		disk.updateInfo();
		DiskInfo info = disk.info();
		ICube shared = disk.home();
		String cubeName = "13113133";
		ICube cube = disk.cube(cubeName);
		cube = disk.createCube(cubeName, null);
		boolean is = disk.existsCube(cubeName);
		List<String> names = disk.enumCube();
		disk.delete();
		disk.close();
	}
	@Test
	public void testInsertObjectId() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("192.168.201.210"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		MongoCollection<Document> col= client.getDatabase("test").getCollection("test");
		Document document=Document.parse("{'name':'zhaoxb'}");
		System.out.println("aaaa:"+document.getObjectId("_id"));
		col.insertOne(document);
		System.out.println("bbbb:"+document.getObjectId("_id"));
		client.close();
	}
	@Test
	public void testCreateDisk() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		CubeConfig conf = new CubeConfig();
		conf.setDimFile(
				"/Users/cj/git/diskcode/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
		conf.setCoordinateFile(
				"/Users/cj/git/diskcode/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
		DiskInfo info = new DiskInfo("cj的网盘", conf);
		INetDisk disk = NetDisk.create(client, "cj1", "cj", "cj",
				info);
		System.out.println("成功" + disk.cubeCount());

	}
	@Test
	public void testInitLnsDisk() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("192.168.201.210"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "$lns.disk", "carocean", "123456");
		ICube home=disk.home();
		
		initUserKeyTools3(home);
//		initUserKeyTools2(home);
		
//		initCloudTerminus(home);
		
	}
	 void initCloudTerminus(ICube home) {
		
		
	}
	 void initUserKeyTools2(ICube home) {
			HashMap<String,String> map=new HashMap<>();
			map.put("user", "zhaoxb");
			map.put("privateKey", "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBALX9XaryhaOFdAQMykWAdOnfBLvKlEqJaT3DyRDQKJU2YpXsdzvVqvA+783zfpmuJh4nV7ley4IB8SBnGADD4ZaALK8CZgSYeEI9Q2m/CVRRgeOG73D7j0dX+H7JZ0W94BM1VvNHoBLNRxYYJWY28E1CqOcW2g+vq2RsihhAI1RdAgMBAAECgYBoJzfrNN8cxayvAK7mdezzR+qKmxahTeEIMzuoPqlrM/PZ/7oBaXhqBGrzwE4NH+i6yyNeeI0Zu4jHVZkcHv7ETb21pCVqvjxzeblR0jXwFN5ZvULeG5o2GoubShi0W/bCUhvyHz8azWLg4EeJeu8b779vvpiNQAttyyqePME6IQJBAPFcl+lmFbaY0L9tYYJaAvQxu0SdZwNtIMhpIQ/BN8l7X/u+4RHpIG4o9aZ6A/MJiUPUWltFjLAS6lbQEtUh+mUCQQDBBvT6fyVDONnifdSq5yXFpKNIV9hG9uns964B6nuX3FcofN7QNLKNAAnikMJ3R2n/LQKiZHSGRsVu4knJEBaZAkEAhri9Xp0Jv6ta0Y8XX3Aot+9ObUVCq8ntA5CS0L10CzWfZOCttpae8H5SZ8MxkyYehUrxyIJwvhNA1IY+DNNzdQJBAL3CBBU55zzlZ7Uz9FyqbcSVrHtS+GgTFjaB6osxo5zP3NMNptFR7PGWRyF3I+W0y+wyqHNShAb5DeCLDvRdH4kCQQDeJ3lZYYVDEpQzrBgNwdPkg1rIVgBGp8vWvOkcxqAP+gPDpsQrga+ha0V6aoBkBqc/f0RsNsBs6R8bjsRDXe0i");
			map.put("publicKey", "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC1/V2q8oWjhXQEDMpFgHTp3wS7ypRKiWk9w8kQ0CiVNmKV7Hc71arwPu/N836ZriYeJ1e5XsuCAfEgZxgAw+GWgCyvAmYEmHhCPUNpvwlUUYHjhu9w+49HV/h+yWdFveATNVbzR6ASzUcWGCVmNvBNQqjnFtoPr6tkbIoYQCNUXQIDAQAB");
			TupleDocument<HashMap<String, String>> tuple=new TupleDocument<HashMap<String,String>>(map);
			home.saveDoc("userKeyTools",tuple);
		}
	void initUserKeyTools(ICube home) {
		HashMap<String,String> map=new HashMap<>();
		map.put("user", "zhaoh");
		map.put("privateKey", "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAJ35qh7wqK9y4LOJgpSvvFpWWeAaGf5AXSojeMsyEqWToqCATfcXd6pwmZUS1B3StZerhdAGWyQigqdqCmfzU8DgXbFs9q3+lKJwLTacc2oex/TLTt7vFQLcw6bT6Nb4o3+hz6y5WaG8e+w8XxIY+PdNCl4eLeh1fXbWv+vnmBfHAgMBAAECgYBzbi13ZCPt50P3DABlQq+fVr9fN9NMa51nn/mwh8sGP4UyP+44IWaoHJSsT9C8Ze2YgJVLNom8MpdDWwF1iV/lRCycDnjSD+bzvRu9aAdmmO+ANiz81lvkA4pgFFr99bSs/sqiZ6eGgdEeCi1ksn8EMe+tt9hIu/7nZb3/2tsFmQJBAON7pdFU0IRxxFxAY5/BfKfmweioIYfnnc5fodOtuJ82NeV7DIcJvkUIooAiNQCxCe8EmnOTWqtU6PbMOBW8QEUCQQCxx2JOv3ksUMp1PrctyaImNu+SP2XYgcRzqsOcbtVSAjt1phi1QV+eTnX0hcm6KCs0KiaGk9uQ4ll4fYL8TlabAkBsakeSxntCQ/4zgTR5tPs2AhI0Ubz5Sne5HXbArbCpoGp7XfOQgCJAQGdB7guYssdrNKRvpLC3qxYEWTv5j+iJAkBNAxhdEKkHP4BUxfRIS36Im2ewrPILljtp9+GBFKooOntTfb5aVpV7WHXUlVhL4jbFBDVoOwj0fHlRrt1dOvpdAkEA3WhYk8zh7RjCCapN9yGxdWXsWHo3NmCHgjWrVD4rzlRYEGvYxnm4B1XxiiQxcgWsDzUOeElzQ08O0kLq8hbVnA==");
		map.put("publicKey", "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCd+aoe8KivcuCziYKUr7xaVlngGhn+QF0qI3jLMhKlk6KggE33F3eqcJmVEtQd0rWXq4XQBlskIoKnagpn81PA4F2xbPat/pSicC02nHNqHsf0y07e7xUC3MOm0+jW+KN/oc+suVmhvHvsPF8SGPj3TQpeHi3odX121r/r55gXxwIDAQAB");
		TupleDocument<HashMap<String, String>> tuple=new TupleDocument<HashMap<String,String>>(map);
		home.saveDoc("userKeyTools",tuple);
	}
	void initUserKeyTools3(ICube home) {
		HashMap<String,String> map=new HashMap<>();
		map.put("user", "sandy");
		map.put("privateKey", "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKqI0rc/gGq5mWnlIo3Q1KDv2A0wUWUwsM/tJ93588lrFZ7YPq1CkRi6wfd2vdvl3q1vcfLCW3xZflBgIZU7IJmyaElU85Iq+LyboHRuNj/m6PNeECfZCW6soa5uGa06b/l2E5QhgF2UWjxtXQs6OVDXCoxTr0uxYcN3KC30OfBnAgMBAAECgYBPc+vo4LcuAkcN5WnR9Qf764MXsflaUfMvDOlULI5+u1uZZFrfUnJTuT+B0lrmxxSWbaQDXLZPG0sUm5VJ+ABZlhREjn/c1iP63A90xekYagUZnDDxQSD7vMfmaVD/toIdIYTDeQ1qgITYEl2s51cIOtvLY6ynShaR0IX1jbJxIQJBAPe+7vxqifOf88tAw4/KXzugDFoyHZUOG9dAkTwmsotGCmemmKCIaK586QhoM3O00zinkDTuJ6DGgOdRTshaeikCQQCwN1c0KVu9EPmCKg8pZKGLVCPjvB9o3qjSoeV21ntABpKdy23qKi5c8OOoYiktMJHKsCIaw2WBQaKEXy4Er4gPAkEA2u/EyxRrrPyhufEV4bB50Htz7xFyyxKYz/SjZIDeL+5Jq6eyIcvqlqiUV3WeYpZeQybxbZU5N/+0Urer46rxYQJATqZ0qphnFMz1wE4LdFsw6yID3sqBbmorCdAuvcrfOeV1HS7GAUUQanUt92LQpBXQjJnurulVbcqgwdpFjNvXqwJAJDAjAMR8Dg/jXq+37tF/8wISSUIxx2QGon+Ytt31tfHzGki2URNnU8UxZpQ6WnUmDnRqxHEm5rORPCKyai3p0A==");
		map.put("publicKey", "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCqiNK3P4BquZlp5SKN0NSg79gNMFFlMLDP7Sfd+fPJaxWe2D6tQpEYusH3dr3b5d6tb3Hywlt8WX5QYCGVOyCZsmhJVPOSKvi8m6B0bjY/5ujzXhAn2QlurKGubhmtOm/5dhOUIYBdlFo8bV0LOjlQ1wqMU69LsWHDdygt9DnwZwIDAQAB");
		TupleDocument<HashMap<String, String>> tuple=new TupleDocument<HashMap<String,String>>(map);
		home.saveDoc("userKeyTools",tuple);
	}
	@Test
	public void testOpenDisk() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "netarea_test", "netos", "11");
		DiskInfo info = disk.info();
		ICube cube=disk.createCube("xxx", new CubeConfig());
		System.out.println(info.alias());
	}

	@Test
	public void testDeleteDisk() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		disk.delete();
//		INetDisk disk2 = NetDisk.open(client, "lish", "lish", "11");
//		disk2.delete();
	}

	@Test
	public void testCountDisk() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		System.out.println(disk.cubeCount() + " " + disk.useSpace() + " "
				+ (disk.dataSize()/1024/1024));
	}

	@Test
	public void testDiskShared() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		TestEntity2 en = new TestEntity2();
		en.ssss = "fuck you.";
		disk.home().saveDoc("fuck", new TupleDocument<TestEntity2>(en));
	}

	@Test
	public void testDiskLoopCreateCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		
		CubeConfig sharedConfi = new CubeConfig();
		sharedConfi.setDimFile(
				"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
		sharedConfi.setCoordinateFile(
				"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
		//DiskInfo info = new DiskInfo("李生的网盘", sharedConfi);
		INetDisk disk2 = NetDisk.open(client, "lish", "lish", "11");
		for (int i = 50; i < 52; i++) {
			CubeConfig conf = new CubeConfig();
			conf.setDimFile(
					"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
			conf.setCoordinateFile(
					"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
			ICube cube = disk2.createCube("我的文件库"+i, conf);
			System.out.println(cube.name());
		}
		System.out.println(disk2.info().alias());
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");

		//耽机原因：文件数限制，MAC默认一个目录下256个文件，以下循环1万次，要创建1万个目录文件，故耽机。
		for (int i = 60; i < 61; i++) {
			CubeConfig conf = new CubeConfig();
			conf.setDimFile(
					"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
			conf.setCoordinateFile(
					"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
			ICube cube = disk.createCube("我的文件库"+i, conf);
			System.out.println(cube.name());
		}
	}

	@Test
	public void testDiskCreateCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");

		CubeConfig conf = new CubeConfig();
		conf.setDimFile(
				"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-dims.bson");
		conf.setCoordinateFile(
				"/Users/carocean/studio/lns/cj.lns.platform/cj.lns.chip.sos.cube/src/cj/lns/chip/sos/cube/test/system-coordinates.bson");
		ICube cube = disk.createCube("我的文件库", conf);
		System.out.println(cube.name());
	}
	@Test
	public void testListDiskCube() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		List<String> list=disk.enumCube();
		for(String name:list){
			System.out.println(name);
		}
	}
	@Test
	public void testDiskCubeSaveDoc() {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		ICube cube = disk.cube("我的文件库0");

		TestEntity2 en = new TestEntity2();
		en.ssss = "fuck you.";
		cube.saveDoc("fuck", new TupleDocument<TestEntity2>(en));
	}

	@Test
	public void testWriteLns() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		ICube cube = disk.cube("我的文件库0");
		FileSystem fs = cube.fileSystem();
		writeLns(fs, new File("/Users/carocean/studio/lns"),"/Users/carocean/studio/lns","/lns/");
	}

	int writeFileCount = 0;

	private void writeLns(FileSystem fs, File file,String local,String root)
			throws FileLockException, IOException {
//		if (writeFileCount > 1000)
//			return;
		String name = file.getAbsolutePath();
		String lname = name.substring(local.length(),
				name.length());
		if (lname.startsWith("/.") || lname.startsWith("/build"))
			return;
		lname=String.format("%s%s", root,lname);
		
		if(lname.endsWith(".class"))return;
		if (file.isDirectory()) {
			fs.dir(lname).mkdir(file.getName());
			File[] files = file.listFiles();
			for (File f : files) {
				writeLns(fs, f,local,root);
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
	public void testReadLns() throws IOException, FileLockException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		INetDisk disk = NetDisk.open(client, "zhaoxb", "carocean", "11");
		ICube cube = disk.cube("我的文件库0");
		FileSystem fs = cube.fileSystem();
		DirectoryInfo root = fs.dir("/lns/");
		String dir="/Users/carocean/Downloads/temp";
		writeDir(dir, root);
		System.out.println("读完");
	}
	private void writeDir(String d,DirectoryInfo parent) throws FileLockException, IOException {
		for (DirectoryInfo dir : parent.listDirs()) {
			System.out.println(dir.path() + " " + dir.name());
			List<FileInfo> files = dir.listFiles();
			for (FileInfo file : files) {
				String fn=String.format("%s/%s/%s",d,dir.path(),file.name());
				System.out.println(file.fullName());
				File f=new File(fn);
				if(!f.getParentFile().exists()){
					f.getParentFile().mkdirs();
				}
				FileOutputStream out=new FileOutputStream(f);
				int read = 0;
				byte[] b = new byte[256 * 1024];
				IReader reader = file.reader(0);
				while ((read = reader.read(b, 0, b.length)) > -1) {
					out.write(b, 0, read);
				}
				reader.close();
				out.close();
			}
			writeDir(d,dir);
		}
	}
}
