package cj.lns.chip.sos.disk;

import java.util.HashMap;
import java.util.Map;

import cj.lns.chip.sos.cube.framework.CubeConfig;

public class DiskInfo {
	String alias;
	String desc;
	Map<String, Object> attrs;
	private CubeConfig shared;

	DiskInfo() {
		attrs = new HashMap<>();
	}
	public String alias() {
		return alias;
	}
	public String desc() {
		return desc;
	}
	public void desc(String desc) {
		this.desc = desc;
	}
	public DiskInfo(String alias,CubeConfig shared) {
		this();
		this.shared = shared;
		this.alias=alias;
	}

	public CubeConfig shared() {
		return shared;
	}

	public void attr(String key, Object value) {
		attrs.put(key, value);
	}

	public Object attr(String key) {
		return attrs.get(key);
	}

	public String[] enumAttr() {
		return attrs.keySet().toArray(new String[0]);
	}

	public boolean containsAttr(String key) {
		return attrs.containsKey(key);
	}

}
