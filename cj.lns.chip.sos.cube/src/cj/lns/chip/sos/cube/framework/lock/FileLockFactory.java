package cj.lns.chip.sos.cube.framework.lock;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import cj.studio.ecm.EcmException;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.util.StringUtil;

/*
 * system_fs_locks集合：
 * {'_id':ObjectId('xxx'),'fileId':'xxx','locker':'eeee','lock':3}//其中handle是加锁者，加锁者在跨平台的并发系统必须唯一，
 * {'_id':ObjectId('xxx'),'fileId':'xxx','locker':'eeee','lock':3}
 */
public class FileLockFactory implements ILockFactory {
	MongoCollection<Document> lockcol;
	ILockPollingStrategy strategy;

	public FileLockFactory(MongoDatabase db) {
		lockcol = db.getCollection("system_fs_locks");
	}

	public void setPollingStrategy(ILockPollingStrategy strategy) {
		this.strategy = strategy;
	}

	@Override
	public void clearAll(Object fileId) {
		BasicDBObject filter = new BasicDBObject("fileId", fileId);
		if (lockcol.count(filter) > 0) {
			lockcol.deleteOne(filter);
		}
	}

	@Override
	public void tryLock(Object source, ILock lock) throws FileLockException {
		tryLock(source, lock, 0);
	}

	private void tryLock(Object source, ILock lock, int pollingtimes)
			throws FileLockException {
		// clear(0),read(1),write(2),readwrite(3),delete(8),none(5)
		// 其中clear(0)表示无锁，不能加clear锁，别的操作不能随随便便清除别人的锁位，不然不乱套了，只能调用clearLocks方法清除文件上的所有锁。
		// 先查source是否持有指定的锁，如果有则直接返回，没有则尝试加锁
		String json = String.format("{'fileId':'%s','lock':%s,'locker':'%s'}",
				lock.getFileId(), lock.getLock(), lock.getLocker());
		Document filter = Document.parse(json);
		if (lockcol.count(filter) > 0) {// 如果是这把锁则直接返回
			FindIterable<Document> find = lockcol.find(filter)
					.projection(new BasicDBObject("_id", 1)).limit(1);
			Document one = find.first();
			String id = ((ObjectId) one.get("_id")).toHexString();
			lock.setId(id);
			return;
		}
		lock(lock.getFileId(), lock, pollingtimes);
	}

	private void lock(String filephyid, ILock lock, int pollingtimes)
			throws FileLockException {
		String json = "";
		Document filter = null;
		switch (lock.getLock()) {
		case 0:
			json = String.format("{'fileId':'%s','lock':{$ne:1}}", filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("不能尝试得到一个off锁，只有在先前是读锁时才可关闭文件共享");
			}
			break;
		case 1:// read,不能是共享写和none
			json = String.format(
					"{'$or':[{'fileId':'%s','lock':2},{'fileId':'%s','lock':4}]}",
					filephyid, filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("加读锁失败，已被共享写或独占");
			}
			break;
		case 2:// write，不能是读和none
			json = String.format(
					"{'$or':[{'fileId':'%s','lock':1},{'fileId':'%s','lock':4}]}",
					filephyid, filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("加写锁失败，已被共享读或独占");
			}
			break;
		case 3:// readwrite
			json = String.format("{'fileId':'%s','lock':4}", filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("加读写锁失败，已被独占");
			}
			break;
		case 8:// delete 如要获取删除锁，必须所有的锁被释放
			json = String.format("{'fileId':'%s'}", filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("加读写锁失败，已被独占");
			}
			break;
		case 4:// none
			json = String.format("{'fileId':'%s'}", filephyid);
			filter = Document.parse(json);
			if (lockcol.count(filter) > 0) {
				if (strategy != null) {
					int counter = strategy.waitPolling(lock);
					for (int i = pollingtimes+1/*因为已经执行一次轮询方法*/; i < counter; i++) {
						tryLock(filephyid, lock,pollingtimes+1);
					}
				}
				throw new FileLockException("加读独占锁失败，已存在锁");
			}
			break;
		default:
			throw new EcmException(String.format("锁键%s不支持。", lock.getLock()));
		}
		Document newItem = Document.parse(new Gson().toJson(lock));
		lockcol.insertOne(newItem);
	}

	@Override
	public void unlock(ILock lock) {
		// 直接删除指定的锁
		String json = "";
		if (StringUtil.isEmpty(lock.getId())) {
			json = String.format("{'fileId':'%s','lock':%s,'locker':'%s'}",
					lock.getFileId(), lock.getLock(), lock.getLocker());
		} else {
			json = String.format(
					"{'$or':[{'_id':ObjectId('%s')},{'fileId':'%s','lock':%s,'locker':'%s'}]}",
					lock.getId(), lock.getFileId(), lock.getLock(),
					lock.getLocker());
		}
		Document filter = Document.parse(json);
		if (lockcol.count(filter) > 0) {
			lockcol.deleteOne(filter);
		}
	}

}
