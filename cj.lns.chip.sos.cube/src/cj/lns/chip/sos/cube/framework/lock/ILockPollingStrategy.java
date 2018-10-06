package cj.lns.chip.sos.cube.framework.lock;
/**
 * 轮询文件锁
 * <pre>
 * 原因：程序无法收到mongodb的通知，只能通过轮询判断引起锁并发异常的文件的锁变更信息。
 * </pre>
 * @author carocean
 *
 */
public interface ILockPollingStrategy {
	/**
	 * 
	 * <pre>
	 *
	 * </pre>
	 * @param lock
	 * @return 返回要轮询的次数
	 */
	int waitPolling( ILock lock);

}
