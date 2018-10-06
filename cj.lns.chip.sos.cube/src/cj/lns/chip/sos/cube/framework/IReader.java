package cj.lns.chip.sos.cube.framework;

public interface IReader {
	/**
	 * 读取
	 * <pre>
	 * 
	 * </pre>
	 * @param b
	 * @param off
	 * @param len
	 * @return 如果返回-1表示文件结末
	 */
	int read(byte[] b, int off, int len) ;
	/**
	 * 读取
	 * <pre>
	 * 
	 * </pre>
	 * @param b
	 * @param off
	 * @param len
	 * @return 如果返回-1表示文件结末
	 */
	int read(byte[] b);
	int read();
	/**
	 * 一次性读出全部数据
	 * <pre>
	 * 限制为10m大小的文件空间，如果超出则异常，
	 * 注意：总是从seek的偏移位开始读取到尾部。如果想读取全部文件，请先调用seek(0)，0表示开头。
	 * </pre>
	 * @return
	 * @throws TooLongException
	 */
	byte[] readFully()throws TooLongException;
	void close();
	/**
	 * 随机定义
	 * <pre>
	 *0为定位开头
	 *-1为文件尾
	 * </pre>
	 * @param pos 是在文件中的偏移字节
	 */
	void seek(long pos);
	/**
	 * 重置到文件首，相当于seek(0)
	 * <pre>
	 *
	 * </pre>
	 */
	void reset();
}
