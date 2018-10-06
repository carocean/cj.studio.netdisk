package cj.lns.chip.sos.cube.framework.lock;
/**
 * 
 * <pre>
 * 先从锁工厂中得到一个锁(已含锁标识为mongo的'_id'），而后要释放锁
 * </pre>
 * @author carocean
 *
 */
public interface ILockFactory {
	void tryLock(Object source,ILock lock)throws FileLockException;
	void unlock(ILock lock);
	void clearAll(Object source);
	void setPollingStrategy(ILockPollingStrategy strategy) ;
}
