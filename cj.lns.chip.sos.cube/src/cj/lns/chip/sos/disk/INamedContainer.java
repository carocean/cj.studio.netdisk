package cj.lns.chip.sos.disk;

import java.util.List;

import com.mongodb.MongoClient;
/**
 * 提供命名空间的容器
 * 
 * <pre>
 * 将立方体的逻辑名，映射为物理名
 * 
 * 共享立方体即网盘默认的立方体，故而不列出。
 * </pre>
 * @author carocean
 *
 */
public interface INamedContainer {
	
	String appendDiskName(MongoClient client, String diskname,DiskInfo info);
	
	String appendDiskName(MongoClient client, String diskname,
			String cubeLogicName);

	boolean existsDiskName(String diskname);

	boolean existsDiskName(String diskname, String cubeName);

	List<String> enumDiskName();
	/**
	 * 获取磁盘中的共享立方体物理名
	 * <pre>
	 *
	 * </pre>
	 * @param name
	 * @return
	 */
	String diskSharedCubePhyName(String diskname);
	/**
	 * 由立方体名得到立方体物理名
	 * <pre>
	 *
	 * </pre>
	 * @param diskname
	 * @param cubeName
	 * @return
	 */
	String diskCubePhyName(String diskname, String cubeName);
	/**
	 * 由立方体物理名得到立方体名
	 * <pre>
	 *
	 * </pre>
	 * @param diskname
	 * @param cubePhyName 物理名
	 * @return
	 */
	String diskCubeName(String diskname, String cubePhyName);
	/**
	 * 
	 * <pre>
	 * 不包含共享立方体，因为它与网盘共同构建，属于网盘的默认立方体
	 * </pre>
	 * @param diskname
	 * @return
	 */
	List<String> enumCubeName(String diskname);

	long countCube(String diskname);
	
	List<String> enumCubePhyName(String diskname);

	DiskInfo diskInfo(String diskname);

	void updateDiskInfo(String diskname, DiskInfo info);

	void removeDisk(String diskname);
	
	void removeCubeName(MongoClient client, String name, String cubeName);

}
