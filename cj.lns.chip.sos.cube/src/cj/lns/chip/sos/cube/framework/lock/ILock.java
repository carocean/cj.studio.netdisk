package cj.lns.chip.sos.cube.framework.lock;

public interface ILock {

	int getLock();

	String getLocker();

	void setId(String id);

	String getFileId();
	String getId();
}
