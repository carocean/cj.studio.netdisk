package cj.lns.chip.sos.cube.framework;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import cj.lns.chip.sos.cube.framework.lock.BlockLock;
import cj.lns.chip.sos.cube.framework.lock.BlockLockFactory;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.cube.framework.lock.FileLockFactory;
import cj.lns.chip.sos.cube.framework.lock.IChunkReader;
import cj.lns.chip.sos.cube.framework.lock.ILock;
import cj.lns.chip.sos.cube.framework.lock.ILockPollingStrategy;
import cj.studio.ecm.EcmException;
import cj.ultimate.util.StringUtil;

/**
 * 废话就不多说了，文件是数据的容器，既然是容器，有满有不满。数而空间大小与数据大小是两码事。
 * 
 * <pre>
 * 你如果要用seek乱写，关我鸟事。
 * </pre>
 * 
 * @author carocean
 *
 */
public class FileInfo extends BaseFile {
	/*
	 * 阶梯块设计：头109M内使用逐次增大块大小方案，以节省空间，但也损耗了一些性能。块越小，读写越慢。
	 * 	块数	数据K	块大小
	0	127	1024	8
	128	383	4096	16
	384	639	8192	32
	640	1151	32768	64
	1152	1663	65536	128
		109	
	1664	9855	2097152	256
		2	
	 */
	long spacelength;
	int[][] blockConfig;
	byte lflag;// lockFlag，1表示文件使用了锁，否则不使用锁机制。
	transient MongoCollection<Document> chunk;
	transient DirectoryInfo parent;
	transient FileLockFactory flock;
	transient BlockLockFactory bllock;
	transient ILock lock;// 表示当前文件的锁实例

	FileInfo() {
	}

	void init(Cube cube, ILock lock) {
		this.cube = cube;
		if (lock != null) {
			flock = new FileLockFactory(cube.cubedb);
			bllock = new BlockLockFactory(new ChunckReader());
			this.lock = lock;
		}
		if (otherCoords == null) {
			otherCoords = new HashMap<>();
		}
		if (blockConfig == null) {
			blockConfig = new int[6][2];
			blockConfig[0][0] = 7;// 0位是块大小，1为是块数
			blockConfig[0][1] = 16;
			blockConfig[1][0] = 15;
			blockConfig[1][1] = 32;
			blockConfig[2][0] = 31;
			blockConfig[2][1] = 32;
			blockConfig[3][0] = 63;
			blockConfig[3][1] = 64;
			blockConfig[4][0] = 127;
			blockConfig[4][1] = 64;
			blockConfig[5][0] = 255;
			blockConfig[5][1] = -1;// -1表示自动扩展
		}

	}


	/**
	 * 全路径名
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @return
	 */
	public String fullName() {
		String dirName = parent().path();
		if (!dirName.endsWith("/")) {
			dirName = String.format("%s/", dirName);
		}
		String path = String.format("%s%s", dirName, name);// .replace("//",
															// "/");
		return path;
	}

	public DirectoryInfo parent() {
		if (parent == null) {
			parent = new DirectoryInfo(coordinate, cube);
		}
		return parent;
	}

	public void clearLocks() {
		Document filter = Document
				.parse(String.format("{'fileId':'%s'}", phyId));
		MongoCollection<Document> lockcol = cube.cubedb
				.getCollection("system_fs_locks");
		lockcol.deleteMany(filter);

		Document filter2 = Document
				.parse(String.format("{'_id':ObjectId('%s')}", phyId));
		Document update2 = Document.parse(String.format("{'lock':%s}", 0));
		chunk.updateMany(filter2, new BasicDBObject("$set", update2));

		BasicDBObject update = new BasicDBObject("tuple.lflag", 0);// -1为独占，将禁用所有读和写操作，0为共享，将可读可写，1为禁用任何写操作但充许任何读操作
		cube.cubedb.getCollection("sysstem_fs_files").updateOne(
				new BasicDBObject("_id", new ObjectId(phyId)),
				new BasicDBObject("$set", update));
	}

	/**
	 * 设置长度
	 * 
	 * <pre>
	 * 如果在文件大小之内则后续被截掉
	 * 如果不在文件大小之内则以空块填充到指定长度
	 * 如果是0则置空文件。
	 * 如果是-1或为其它负数则表示为当前文件长度因此不作任何操作
	 * 
	 * 注意：如果使用了锁机制，在设置空间时推荐以独占方式打开文件。
	 * </pre>
	 * 
	 * @param len
	 * @throws FileLockException
	 */
	public void setSpaceLength(long len) throws FileLockException {
		// this.writebpos =
		// pos;//pos是文件内字节偏移地址，writebpos是块偏移地址，要计算出块并取出该块，然后计算出在块中的偏移量，然后：
		// 自定位处，其后的块删除，USEDSIZE设到该定位处。因此小心使用seek
		if (len < 0) {
			return;
		}
		try {
			if (this.lflag == 1) {
				flock.tryLock(this, lock);
			}
			if (len == 0) {// 置空
				Document filter = Document
						.parse(String.format("{'fileId':'%s'}", phyId));
				chunk.deleteMany(filter);
			} else {
				/*
				 * 以下实现的不足：
				 *  	当文件中的数据块在后台库中被删除，却无法修复（比如补上空块）
				 * 好处是：
				 * 		性能好
				 * 另一方案，摸拟read方法的实现机制，但不读出chunk而是判断块是否存在，从第0块到尾块，不存在则新建，存在则什么也不做，如果偏移超出尾块，则直接新建块。
				 * 这类似于从头开始写空块一样。但由于要去检查存在的块，如果文件太大一定会导致性能问题。
				 */
				Offset end = getOffset(-1, blockConfig);
				Offset offset = getOffset(len - 1, blockConfig);
				if (offset.inner(end)) {// 删除后续块，当前位置的块设置usedSize
					Document filter = Document.parse(
							String.format("{'fileId':'%s','bnum':{$gt:%s}}",
									phyId, offset.blockNum));
					chunk.deleteMany(filter);
					Chunk ch = this.readChunk(offset.blockNum);
					ch.setUsedSize(offset.offset + 1);
					updateChunk(ch);
				} else {// 添加后续空块。
					long blocks = offset.blockNum - end.blockNum;
					if (blocks > 0) {
						for (long num = end.blockNum
						/*+ 1*/; num <= offset.blockNum; num++) {
							Chunk ch = newBlock(num);
							insertChunk(ch);
						}
					}
				}
			}
		} catch (FileLockException e) {
			throw e;
		} finally {
			spacelength = len;
			if (lflag == 1) {
				flock.unlock(lock);
			}
			updateFileMeta();
		}

	}

	/**
	 * 文件空间长度
	 * 
	 * <pre>
	 * 注意：文件空间长度并不一定等同于文件数据长度，一般情况下相等，仅当调用了setLength使文件变长，则只是扩展了空间，而数据长度未变。
	 * </pre>
	 * 
	 * @return
	 */
	public long spaceLength() {
		return spacelength;
	}

	/**
	 * 文件中实际存储的数据长度
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * 文件到1g的时候统计得太慢
	 * 
	 * @return
	 */
	// public long dataLength1() {
	// Offset end = getOffset(-1, blockConfig);
	// if(end.blockNum==0&&end.offset==0)return 0;//说明为空
	// long len = 0;
	// for (int i = 0; i < end.blockNum + 1; i++) {
	// Chunk ch = readChunk(i);
	// len += ch.getUsedSize();
	// }
	// return len;
	// }
	public long dataLength() {// 先查出块的头，然后计算
//		Offset end = getOffset(-1, blockConfig);//计算偏移地址超慢，发现是查块并排序问题
//		if (end.blockNum == 0 && end.offset == 0)
//			return 0;// 说明为空
		if(spacelength==0)return 0;//因为上面注释掉的代码超慢，因此不再计算尾号。文件空间长度为0肯定是没数了。
		long len = 0;
		Document filter = Document
				.parse(String.format("{'fileId':'%s'}", phyId));
		FindIterable<Document> find = chunk.find(filter)
				.projection(new BasicDBObject("block", 1))
				/*.sort(new BasicDBObject("bnum", 1))*/;//没必要排序，则是乱了也不影响统计数据大小。
		for(Document doc:find){
			long block=doc.getLong("block");
			len+=(int)(block&0xFFFFFFL);
		}
		return len;
	}

	public IWriter writer(long pos) throws FileLockException {
		if (this.lflag == 1) {
			flock.tryLock(this, lock);
		}
		Offset offset = getOffset(pos, blockConfig);// 将pos转换为块
		return new Writer(offset);// lock给写入器，等写入器关闭时解除lock
	}

	public void setLockPollingStrategy(ILockPollingStrategy strategy) {
		if (lflag == 1) {
			flock.setPollingStrategy(strategy);
		}

	}

	private void updateChunk(Chunk ch) {
		Document filter = Document
				.parse(String.format("{'_id':ObjectId('%s')}", ch.phyId));
		chunk.updateOne(filter, new BasicDBObject("$set", ch.toDoc()));
	}

	private void insertChunk(Chunk ch) {
		chunk.insertOne(ch.toDoc());
	}

	private Chunk convertChunk(Document one) {
		Chunk ch = new Chunk(one.getString("fileId"), one.getLong("bnum"),
				((Binary) one.get("b")).getData(), one.getLong("block"));
		ch.phyId = ((ObjectId) one.get("_id")).toHexString();
		return ch;
	}

	private Chunk newBlock(long bnum) {
		int blockSize = assignBlockSize(bnum, blockConfig);
		// System.out.println(String.format("新块%s大小：%s", bnum, blockSize));
		Chunk ch = new Chunk(phyId, bnum, new byte[blockSize], 0);
		return ch;
	}

	// pos=-1表示找尾块，0为第一块
	// pos是字节位
	// 参见文档：核对文件随机定位.numbers
	
	private Offset getOffset(long pos, int[][] conf) {
		if (pos == -1) {//注意：在计算尾时查询超慢，因为mongodb是将所有的符合条件的数查出后再排序
			Document filter = Document
					.parse(String.format("{'fileId':'%s'}", phyId));
			Document sort = Document.parse("{'bnum':-1}");
			FindIterable<Document> find = chunk.find(filter)
					.sort(sort).limit(1);
			Document last = find.first();/// 取出最后一条记录
			if (last == null) {// 表示文件为空
				return new Offset(0, 0);
			}
			Chunk ch = convertChunk(last);
			// return new Offset(ch.bnum,
			// ch.getUsedSize()-1);//结尾是块的长度而非是数据长度，因为在本系统中，文件仅是容器，其中的数据可以不连续。
			return new Offset(ch.bnum, ch.getBlockSize() - 1);
		}
		if (pos == 0) {
			return new Offset(0, 0);
		}
		// 计算出块号
		long prelen = 0;// 先前的长度
		long len = 0;// 长度
		int autosize = 0;
		long num = 0;// 块号
		int offset = 0;// 块内偏移
		long demandsize = pos + 1;// 文件偏移处的长度
		for (int i = 0; i < conf.length; i++) {
			int[] b = conf[i];
			if (b[1] == -1) {
				autosize = b[0] * 1024;
				break;
			}
			for (int j = 0; j < b[1]; j++) {
				prelen = len;
				len += (b[0] * 1024);
				if (demandsize > prelen && demandsize <= len) {
					offset = (int) (demandsize - prelen - 1);
					return new Offset(num, offset);
				}
				num++;
			}
		}
		long freeauto = pos - len;
		num += freeauto / autosize;
		offset = (int) (freeauto % autosize);
		return new Offset(num, offset);
	}

	// pos是块号
	/**
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @param bnum
	 *            是块号
	 * @param conf
	 * @return 返回的是块大小
	 */
	private int assignBlockSize(long bnum, int[][] conf) {
		int preNum = 0;
		int num = 0;
		for (int i = 0; i < conf.length; i++) {
			int[] b = conf[i];
			if (b[1] == -1) {
				return b[0] * 1024;
			}
			preNum = num;
			num += b[1];
			if (bnum >= preNum && bnum < num) {
				return b[0] * 1024;
			}
		}
		throw new EcmException("分配块空间出错。");
	}

	private synchronized void updateFileMeta() {
		TupleDocument<FileInfo> tuple = new TupleDocument<FileInfo>(this);
		cube.updateDoc("system_fs_files", phyId, tuple);
	}

	private Chunk readChunk(long blockNum) {
		Document filter = Document.parse(
				String.format("{'fileId':'%s','bnum':%s}", phyId, blockNum));
		FindIterable<Document> find = chunk.find(filter).limit(1);
		Document one = find.first();
		if (one == null) {
			// throw new EcmException("文件损坏，发现不连继块，可能已丢失：" + blockNum);
			return null;
		}
		return convertChunk(one);
	}

	public IReader reader(long pos) throws FileLockException {
		if (spacelength != 0 && pos >= spacelength) {
			throw new EcmException(
					String.format("设定的值超出文件空间长度:%s", spaceLength()));
		}
		if (this.lflag == 1) {
			flock.tryLock(this, lock);
		}
		Offset end = getOffset(-1, blockConfig);
		Offset seek = getOffset(pos, blockConfig);
		return new Reader(end, seek);
	}

	/**
	 * 强制删除（不论是否有锁，包括独点锁均可被强制删除 ）
	 * 
	 * <pre>
	 *
	 * </pre>
	 */
	public void delete() {
		try {
			delete(true);
		} catch (FileLockException e) {
			throw new EcmException(e);
		} finally {
		}
	}

	public void delete(boolean force) throws FileLockException {
		try {
			if (lflag == 1 && !force) {
				flock.tryLock(this, lock);
			}
			Document filter = Document
					.parse(String.format("{'fileId':'%s'}", phyId));
			chunk.deleteMany(filter);
			MongoCollection<Document> col = cube.cubedb
					.getCollection("system_fs_files");
			filter = Document
					.parse(String.format("{'_id':ObjectId('%s')}", phyId));
			col.deleteOne(filter);
			//检查块集合记录数是否为0，如果是则删除块集合。不删除块集合也可以，因为它可以反复使用，因此不必删除
		} catch (FileLockException e) {
			throw e;
		} finally {
			if (lflag == 1 && !force) {
				flock.unlock(lock);
			}
		}
	}

	class Writer implements IWriter {
		transient Offset offset;// 所在块位置
		Chunk block;

		public Writer(Offset offset) {
			this.offset = offset;
		}

		@Override
		public void seek(long pos) {
			if (pos >= spacelength) {
				throw new EcmException(
						String.format("设定的值超出文件空间长度:%s", spaceLength()));
			}
			Offset off = getOffset(pos, blockConfig);
			offset.blockNum = off.blockNum;
			offset.offset = off.offset;
		}

		@Override
		public void close() {
			if (block != null) {
				block.setUsedSize(offset.offset);
				if (StringUtil.isEmpty(block.phyId)) {
					insertChunk(block);
				} else {
					updateChunk(block);
				}
			}
			updateFileMeta();
			if (lflag == 1) {
				flock.unlock(lock);
			}
			block = null;
		}

		@Override
		public void write(byte[] b) {
			write(b, 0, b.length);
		}

		@Override
		public synchronized void write(byte[] b, int off, int len) {
			for (int i = off; i < len; i++) {
				write(b[i] & 0xFF);
			}
			// updateFileMeta();
		}

		// 字节转整数(byte&0xFF)反转（byte）int，如果 是-1表示结束
		public synchronized void write(int b) {
			if (block == null || offset.blockNum != block.bnum) {
				block = readChunk(offset.blockNum);
				if (block == null) {
					block = newBlock(offset.blockNum);
					if (lflag == 1) {
						ILock newLock = new BlockLock(phyId, null, 1);// 尝试写
						try {
							bllock.tryLock(block, newLock);
						} catch (FileLockException e) {
							throw new EcmException(e);
						} finally {
							bllock.clearAll(block);
						}
					}
					spacelength += block.getBlockSize();
				}
			}
			byte[] data = block.b;
			if (offset.offset < block.getBlockSize()) {// 此处是唯一一处写入操作
				data[offset.offset] = (byte) b;
				// spacelength++;//注掉原因：设文件空间长10，而从0位开始写，写一个字节此处却将空间长度加一个字节因此变为11，12，13。。。这是错误的，改在newBlock时才记录扩展的空间。
				offset.offset++;
			} else {
				block.setUsedSize(offset.offset);
				if (StringUtil.isEmpty(block.phyId)) {
					insertChunk(block);
				} else {
					updateChunk(block);
				}
				offset.blockNum++;
				offset.offset = 0;
				write(b);
			}

		}

	}

	class Reader implements IReader {
		Offset end;
		transient Offset offset;
		Chunk block;

		public Reader(Offset end, Offset offset) {
			this.end = end;
			this.offset = offset;
		}

		@Override
		public void reset() {
			seek(0);
			block = null;
		}

		@Override
		public void seek(long pos) {
			if (pos >= spacelength) {
				throw new EcmException(
						String.format("设定的值超出文件空间长度:%s", spaceLength()));
			}
			Offset off = getOffset(pos, blockConfig);
			offset.blockNum = off.blockNum;
			offset.offset = off.offset;
		}

		@Override
		public void close() {
			if (lflag == 1) {
				flock.unlock(lock);
			}
		}

		@Override
		public int read(byte[] b) {
			return read(b, 0, b.length);
		}

		@Override
		public byte[] readFully() throws TooLongException {
			if (spacelength == 0) {
				return new byte[0];
			}
			if (spacelength > 10485760) {// 此处不用datalength是因为datalength要计算
				// 还是要用dataLength，因为readFully往往调用一次，又不是像read方法要多次调用，因此多一次统计没关系。
				if (dataLength() > 10485760) {
					throw new TooLongException("文件可能较大，因为其空间长度超出10M");
				}
			}
			int blen = 10240;
			ByteArrayOutputStream buf = new ByteArrayOutputStream(blen);
			int timeRead = 0;
			byte[] b = new byte[blen];
			while ((timeRead = read(b, 0, blen)) > -1/*原先为0,由于改了read(byte[] b, int off, int len)方法肯定在结束时返回－1因此改为－1*/) {
				buf.write(b, 0, timeRead);
			}
			return buf.toByteArray();
		}

		@Override
		public synchronized int read(byte[] b, int off, int len) {
			if (spacelength == 0 || !offset.inner(end)) {
				return -1;
			}
			int readtimes = 0;
			for (int i = off; i < len; i++) {
				int d = read();
				if (d == -1) {// 如果中间有返回还是得将前几次循环的readtimes返回，如果直接返回方法则丢失
					if(readtimes==0){//说明本次什么都没读却发现了结尾，因此返回－1,如果有读取的字节而发现了结尾，则返回读取的字节，下次再读时则会出现：读取0个字节则返回了－1因此此时返回－1
						return -1;
					}
					break;
				}
				b[i] = (byte) d;
				readtimes++;
			}
			return readtimes;
		}

		// 字节转整数(byte&0xFF)反转（byte）int，如果 是-1表示结束
		public synchronized int read() {
			if (spacelength == 0 || !offset.inner(end)) {
				return -1;
			}
			if (block == null || offset.blockNum != block.bnum) {
				block = readChunk(offset.blockNum);
				if (lflag == 1) {
					ILock newLock = new BlockLock(phyId, null, 0);// 尝试读
					try {
						bllock.tryLock(block, newLock);
					} catch (FileLockException e) {
						throw new EcmException(e);
					} finally {
						bllock.clearAll(block);
					}
				}
				if (block == null) {
					throw new EcmException(
							"文件损坏，发现不连继块，可能已丢失：" + offset.blockNum);
				}
			}
			byte[] data = block.b;
			if (offset.offset < block.getUsedSize()) {
				byte b = data[offset.offset];
				offset.offset++;
				return b & 0xFF;
			} else {
				offset.blockNum++;
				offset.offset = 0;
				// System.out.println("++++翻页：" + offset.blockNum);
				return read();
			}

		}

	}

	class Offset {
		public Offset(long blockNum, int offset) {
			this.blockNum = blockNum;
			this.offset = offset;
		}

		public boolean inner(Offset end) {
			if (blockNum > end.blockNum)
				return false;
			if (blockNum == end.blockNum) {
				return offset <= end.offset;
			}
			if (blockNum < end.blockNum) {
				return true;
			}
			return false;
		}

		long blockNum;
		int offset;

		@Override
		public boolean equals(Object obj) {
			Offset off = (Offset) obj;
			return blockNum == off.blockNum && offset == off.offset;
		}
	}

	class ChunckReader implements IChunkReader {
		@Override
		public Chunk read(String fileId, long blockNum) {
			return readChunk(blockNum);
		}

		@Override
		public void updateBlockLock(long bnum, long blockLock) {
			Document filter = Document
					.parse(String.format("{'_id':ObjectId('%s')}", phyId));
			Document update = Document
					.parse(String.format("{'lock':%s}", blockLock));
			chunk.updateOne(filter, new BasicDBObject("$set", update));

		}
	}

}
