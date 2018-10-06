package cj.lns.chip.sos.cube.framework.lock;
//clear(0),read(1),write(2),readwrite(3),none(4)
public enum OpenShared {
	/**
	 * 关闭共享锁机制,必须在先前共享读的并发下才可关闭共享机制
	 */
	off(0),
	/**
	 * 以共享读方式打开文件,即所有操作者必须以读或读写共享方式打开
	 */
	read(1),write(2),readwrite(3),delete(8),
	/**
	 * 以独占方式打开
	 */
	none(4);
	int value;
	private OpenShared(int value) {
		this.value=value;
	}
	public int getValue() {
		return value;
	}
}
