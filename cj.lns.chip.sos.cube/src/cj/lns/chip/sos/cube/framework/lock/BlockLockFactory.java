package cj.lns.chip.sos.cube.framework.lock;

import cj.lns.chip.sos.cube.framework.Chunk;
import cj.studio.ecm.EcmException;

/*
 * 数据块级的锁均是排它锁。
 * 0，表示无锁
 * 1，表示排它锁
 */
public class BlockLockFactory implements ILockFactory {
	ILockPollingStrategy strategy;
	private IChunkReader reader;

	public BlockLockFactory(IChunkReader reader) {
		this.reader = reader;
	}

	// source为数组：0是块读取器，1是块
	@Override
	public void tryLock(Object source, ILock lock) throws FileLockException {
		tryLock(source, lock, 0);
	}

	private void tryLock(Object source, ILock lock, int pollingtimes)
			throws FileLockException {
		Chunk ch = (Chunk) source;
		if (ch.getLock() == 1) {
			if (strategy != null) {
				int counter = strategy.waitPolling(lock);
				for (int i = pollingtimes+1; i < counter; i++) {
					ch = reader.read(lock.getFileId(), ch.getBnum());
					tryLock(ch, lock);
				}
			}
			throw new FileLockException("为数据块加读独占锁失败，已存在锁。块号："+ch.getBnum());
		}
		ch.setLock(lock.getLock());
		reader.updateBlockLock(ch.getBnum(),ch.getLock());
	}

	@Override
	public void unlock(ILock lock) {
		throw new EcmException("块工厂不支持该方法，可用clearAll来释放块级锁。");
	}

	@Override
	public void clearAll(Object source) {
		Chunk ch = (Chunk) source;
		ch.setLock(0);
		reader.updateBlockLock(ch.getBnum(),ch.getLock());
	}

	@Override
	public void setPollingStrategy(ILockPollingStrategy strategy) {
		this.strategy = strategy;
	}

}
