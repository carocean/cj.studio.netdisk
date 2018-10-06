package cj.lns.chip.sos.cube.framework.lock;

public class BlockLock implements ILock {
	String fileId;
	String locker;
	byte lock;
	public BlockLock(String filephyid,String locker, int lock) {
		this.fileId=filephyid;
		this.lock=(byte)lock;
		this.locker=locker;
	}

	public void setId(String id) {
		
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
		// TODO Auto-generated method stub
		return null;
	}
}