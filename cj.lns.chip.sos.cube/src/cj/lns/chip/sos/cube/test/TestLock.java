package cj.lns.chip.sos.cube.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import cj.lns.chip.sos.cube.framework.Cube;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IReader;
import cj.lns.chip.sos.cube.framework.IWriter;
import cj.lns.chip.sos.cube.framework.OpenMode;
import cj.lns.chip.sos.cube.framework.TooLongException;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.cube.framework.lock.ILock;
import cj.lns.chip.sos.cube.framework.lock.ILockPollingStrategy;
import cj.lns.chip.sos.cube.framework.lock.OpenShared;

public class TestLock {
	@Test
	public void testReadFileBingFa()
			throws IOException, FileLockException, TooLongException {
		List<ServerAddress> seeds = new ArrayList<>();
		seeds.add(new ServerAddress("localhost"));
		List<MongoCredential> credential = new ArrayList<>();
		MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClient client = new MongoClient(seeds, credential, options);
		ICube cube = Cube.open(client, "zhaoxb");
		FileSystem fs = cube.fileSystem();
		String fn = "/cds/demo/terminaldevice/node-webkit-整合jquery及打包.txt";
		FileInfo file1 = fs.openFile(fn, OpenMode.onlyOpen, OpenShared.read);
		file1.clearLocks();

		IReader reader = file1.reader(0);
		System.out.println(new String(reader.readFully()));
		IWriter writer = file1.writer(-1);
		writer.write("22".getBytes());
		 reader.close();

		file1 = fs.openFile(fn, OpenMode.onlyOpen, OpenShared.write);
		file1.setLockPollingStrategy(new ILockPollingStrategy() {

			@Override
			public int waitPolling(ILock lock) {
				System.out.println("锁并发轮询:");
				return 10;
			}
		});
		reader = file1.reader(0);
		System.out.println(new String(reader.readFully()));
		reader.close();

		System.out.println("读完");
	}
}
