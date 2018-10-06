package cj.lns.chip.sos.disk;

import java.util.List;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.Cube;
import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.studio.ecm.EcmException;
import cj.ultimate.util.StringUtil;

public class NetDisk implements INetDisk {
	String name;
	private MongoClient client;
	private DiskInfo info;
	static INamedContainer container;

	private NetDisk(String name, DiskInfo info, MongoClient client) {
		this.name = name;
		this.client = client;
		this.info = info;
	}

	public String name() {
		return name;
	}
	/**
	 * 打开磁盘
	 * <pre>
	 *
	 * </pre>
	 * @param client
	 * @param name 磁盘名
	 * @param userName
	 * @param password
	 * @return
	 */
	public static INetDisk open(MongoClient client, String name,
			String userName, String password) {
		if (container == null) {
			container = new NameContainer(client);
		}
		if (!existsDisk(client, name)) {
			throw new EcmException(String.format("不存在网盘：%s", name));
		}
		DiskInfo info = container.diskInfo(name);
		if (!verifyPassword(info, userName, password)) {
			throw new EcmException("认证失败。");
		}
		NetDisk disk = new NetDisk(name, info, client);
		return disk;
	}
	/**
	 * 受信打开方式
	 * <pre>
	 *
	 * </pre>
	 * @param client
	 * @param name 磁盘名
	 * @return
	 */
	public static INetDisk trustOpen(MongoClient client, String name) {
		if (container == null) {
			container = new NameContainer(client);
		}
		if (!existsDisk(client, name)) {
			throw new EcmException(String.format("不存在网盘：%s", name));
		}
		DiskInfo info = container.diskInfo(name);
		NetDisk disk = new NetDisk(name, info, client);
		return disk;
	}
	private static boolean verifyPassword(DiskInfo info, String userName,
			String password) {
		return userName.equals(info.attr("userName"))
				&& password.equals(info.attr("password"));
	}
	
	// 网盘维持系统盘，系统盘用于存网盘名，而一个网盘实例有一个内置立方体和共享立体
	public static INetDisk create(MongoClient client, String name,
			String userName, String password, DiskInfo info) {
		if (existsDisk(client, name)) {
			throw new EcmException(String.format("已存在网盘：%s", name));
		}
		info.attrs.put("userName", userName);
		info.attrs.put("password", password);
		container.appendDiskName(client, name, info);
		
		NetDisk disk = new NetDisk(name, info, client);

		CubeConfig sharedconf = info.shared();
		String sharedDbName = container.diskSharedCubePhyName(name);
		ICube shared = Cube.create(client, sharedDbName, sharedconf);
		shared.close();

		return disk;
	}

	public static boolean existsDisk(MongoClient client, String diskname) {
		if (container == null) {
			container = new NameContainer(client);
		}

		return container.existsDiskName(diskname);// 要用系统立方体中维护的盘名关系判断。
	}

	public static List<String> enumDisk(MongoClient client) {
		if (container == null) {
			container = new NameContainer(client);
		}
		return container.enumDiskName();
	}
	@Override
	public void deleteCube(String cubeName){
		if("home".equalsIgnoreCase(cubeName)){
			throw new EcmException("home存储空间不能删除");
		}
		if (!container.existsDiskName(name, cubeName)) {
			throw new EcmException(
					String.format("网盘%s中不存在存储空间%s", name, cubeName));
		}
		String phyname = container.diskCubePhyName(name, cubeName);
		ICube cube= Cube.open(client, phyname);
		container.removeCubeName(client,name,cubeName);
		cube.deleteCube();
	}
	@Override
	public ICube createCube(String cubeName, CubeConfig conf) {
		if("home".equalsIgnoreCase(cubeName)){
			throw new EcmException("home存储空间是保留名字");
		}
		if (container.existsDiskName(name, cubeName)) {
			throw new EcmException(
					String.format("网盘%s中已存在立方体%s", name, cubeName));
		}
		String cubedbName = container.appendDiskName(client, name, cubeName);
		if (!StringUtil.isEmpty(conf.alias())) {
			conf.alias(conf.alias());
		} else {
			conf.alias(cubeName);
		}
		ICube cube = Cube.create(client, cubedbName, conf);
		return cube;
	}

	@Override
	public void delete() {
		List<String> phynames = container.enumCubePhyName(name);
		for (String n : phynames) {
			client.dropDatabase(n);
		}

		container.removeDisk(name);
	}

	@Override
	public void close() {
		this.info = null;
		this.client = null;
	}

	@Override
	public DiskInfo info() {
		return info;
	}

	@Override
	public ICube home() {
		String phyname = container.diskSharedCubePhyName(name);
		return Cube.open(client, phyname);
	}

	@Override
	public ICube cube(String cubeName) {
		if("home".equals(cubeName)){
			return home();
		}
		String phyname = container.diskCubePhyName(name, cubeName);
		return Cube.open(client, phyname);
	}
	public String getCubeName(String cubePhyName){
		return container.diskCubeName(name, cubePhyName);
	}
	@Override
	public void updateInfo() {
		container.updateDiskInfo(name, info);

	}

	@Override
	public List<String> enumCube() {
		return container.enumCubeName(name);
	}

	@Override
	public long cubeCount() {
		return container.countCube(name);
	}

	@Override
	public boolean existsCube(String cubeName) {
		return container.existsDiskName(name, cubeName);
	}

	@Override
	public double dataSize() {
		List<String> names = container.enumCubePhyName(name);
		double ret = 0;
		for (String n : names) {
			ICube cube = Cube.open(client, n);
			ret += cube.dataSize();
		}
		return ret;
	}

	@Override
	public double useSpace() {
		List<String> names = container.enumCubePhyName(name);
		double ret = 0;
		for (String n : names) {
			ICube cube = Cube.open(client, n);
			ret += cube.usedSpace();
		}
		return ret;
	}
}
