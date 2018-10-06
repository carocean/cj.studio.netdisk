package cj.lns.chip.sos.cube.framework.lock;

public class FileLock implements ILock {
	transient String id;
	String fileId;
	String locker;
	byte lock;
	public FileLock(String filephyid,String locker, int lock) {
		this.fileId=filephyid;
		this.lock=(byte)lock;
		this.locker=locker;
	}

	public void setId(String id) {
		this.id=id;
		
	}
	@Override
	public int getLock() {
		return lock&0xFF;
	}
	@Override
	public String getLocker() {
		// TODO Auto-generated method stub
		return locker;
	}
	public String getFileId() {
		return fileId;
	}
	@Override
	public String getId() {
		return id;
	}
}