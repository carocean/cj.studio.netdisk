package cj.lns.chip.sos.cube.framework;

public enum OpenMode {
	/**
	 * 如果已存在，则重写文件（先设空再写）
	 */
	createNew,
	/**
	 * 如果不存在则异常
	 */
	onlyOpen,
	/**
	 * 如果存在则打开，不存在则创建，打开时读操作从头，写操作自尾
	 */
	openOrNew
}
