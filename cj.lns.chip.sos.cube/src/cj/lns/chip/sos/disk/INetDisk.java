package cj.lns.chip.sos.disk;

import java.util.List;

import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.cube.framework.ICube;

/**
 * 网盘服务，单例模式
 * 
 * <pre>
 * ~ 管理数个立方体，有：主立方体（可见）及其它数据立方体。
 * ~ 网盘建立时自动创建主立方体。
 * 
 * 注意：由于网盘创建大量的立方体，mongodb每个库产生一个文件目录，因此在根下会产生大量的文件目录，如果操作系统不取消文件数限制，会导致mongodb服务器退出。ulimit -n 65536
 * </pre>
 * 
 * @author carocean
 *
 */
public interface INetDisk {
	double useSpace();
	String name();
	double dataSize();

	void delete();

	void close();

	DiskInfo info();
	
	ICube home();
	String getCubeName(String cubePhyName);
	ICube cube(String cubeName);
	/**
	 * 
	 * <pre>
	 * 注意：名列表中并不包含共享立方体。
	 * </pre>
	 * @return
	 */
	List<String> enumCube();
	/**
	 * 包括共享立方体在内的所有立方体
	 * <pre>
	 *
	 * </pre>
	 * @return
	 */
	long cubeCount();

	boolean existsCube(String cubeName);

	ICube createCube(String cubeName, CubeConfig conf);

	void updateInfo();
	void deleteCube(String cubeName);

}
