package cj.lns.chip.sos.cube.framework;

import org.bson.Document;
import org.bson.types.Binary;

import cj.studio.ecm.EcmException;

public class Chunk {
	transient String phyId;
	String fileId;
	long block;// 低0-2个字节是已用空间，第3-5字节是块大小，6-7字节是空闲,第6字节的低四位是块级锁
	byte lock;//1为独占，0为可读写
	long bnum;
	byte[] b;
	
	public Chunk(String fileId, long bnum, byte[] b,long block) {
		super();
		this.fileId = fileId;
		this.bnum = bnum;
		this.b = b;
		this.block=block;
		setBlockSize(b.length);
	}
	public static void main(String...strings){
		Chunk ch=new Chunk("", 0, new byte[100], 0);

		ch.setBlockSize(33333);
//		ch.setBlockSize(5555);
		System.out.println(ch.getBlockSize());
		ch.setLock(5);
//		ch.setLock(8);
		System.out.println(ch.getLock());
//		ch.setUsedSize(2323);
		ch.setUsedSize(4444);
//		ch.setUsedSize(44424);
		System.out.println(ch.getUsedSize());
		
		
	}
	public long getBlockHead() {
		return block;
	}
	public void setLock(int lock){
		this.lock=(byte)lock;
	}
	public int getLock() {
		return lock;
	}
//	public void setLock(int lock){
//		if(lock>0xF){
//			throw new EcmException("超出锁的定义范围，取值为：0-15：");
//		}
//		block&=~0xF000000000000L;
//		block |= ((lock*1L) <<48);
//	}
//	public int getLock() {
//		return (int)((block&0xF000000000000L)>>48);
//	}
	public void setUsedSize(int size) {// 低三个字节
		if (size > 0xFFFFFFL) {
			throw new EcmException("已用空间溢出,最大："+0xFFFFFFL);
		}
		block&=~0xFFFFFFL;
		block |= size;
	}

	private void setBlockSize(int size) {// 继低三个字节
		if (size > 0xFFFFFF000000L) {
			throw new EcmException("块空间溢出，最大:"+0xFFFFFF000000L);
		}
		block&=~0xFFFFFF000000L;
		block |= ((size*1L) << 24);
	}

	public int getUsedSize() {
		return (int)(block&0xFFFFFFL);
	}

	public int getBlockSize() {
		return (int)((block&0xFFFFFF000000L)>>24);
	}

	public String getPhyId() {
		return phyId;
	}

	public void setPhyId(String phyId) {
		this.phyId = phyId;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public long getBnum() {
		return bnum;
	}

	public void setBnum(long bnum) {
		this.bnum = bnum;
	}

	public byte[] getB() {
		return b;
	}

	public void setB(byte[] b) {
		this.b = b;
	}

	public Document toDoc() {
		Document doc = new Document();
		doc.put("fileId", fileId);
		doc.put("bnum", bnum);
		doc.put("b", new Binary(b));
		doc.put("block", block);
		return doc;
	}
}
