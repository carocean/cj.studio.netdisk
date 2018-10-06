package cj.lns.chip.sos.cube.framework;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BaseFile {
	transient protected String phyId;
	protected Date createDate;
	protected String name;
	protected transient Cube cube;
	protected transient Coordinate coordinate;
	protected transient Map<String,Coordinate> otherCoords;
	
	protected Map<String, String> attrs;
	public BaseFile() {
		attrs= new HashMap<>();
	}
	public Set<String> enumCoordinate(){
		return otherCoords.keySet();
	}
	public Coordinate coordinate(String dimName){
		return otherCoords.get(dimName);
	}
	public boolean containsCoordinate(String dimName){
		return otherCoords.containsKey(dimName);
	}
	public void attr(String key,String value){
		attrs.put(key, value);
	}
	public String attr(String key){
		return attrs.get(key);
	}
	public Set<String> enumAttr(){
		return attrs.keySet();
	}
	public String name() {
		return name;
	}
	public Date createDate() {
		return createDate;
	}
	public String phyId() {
		return phyId;
	}
}
