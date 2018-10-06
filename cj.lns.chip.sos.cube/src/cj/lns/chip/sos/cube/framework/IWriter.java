package cj.lns.chip.sos.cube.framework;

public interface IWriter {
	/**
	 * 一定要关闭，否则可能会丢失数据或文件损失
	 * <pre>
	 *
	 * </pre>
	 */
	void close();
	/**
	 * 随机定义
	 * <pre>
	 * ~ 0为定位开头，覆盖模式可用
	 * ~ -1为文件尾，追加模式可用
	 *   	说明：如果空间长度是15379456，定位到它的末尾位便是15379455，之后在读写时指定下移至15379456开始，因此不会影响15379455位置原来的数据，故为追加模式
	 * </pre>
	 * @param pos 是在文件中的偏移字节
	 */
	void seek(long pos);
	/**
	 * 写入数据
	 * <pre>
	 *
	 * </pre>
	 * @param b
	 * @param off
	 * @param len
	 */
	void write(byte[] b, int off, int len);
	void write(byte[] b);
	void write(int b);
}
