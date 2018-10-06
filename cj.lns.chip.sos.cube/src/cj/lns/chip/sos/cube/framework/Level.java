package cj.lns.chip.sos.cube.framework;

import cj.studio.ecm.EcmException;

public class Level {
	Property property;//一个级别由一个属性表示
	Level nextLevel;
	private Level parent;
	public Level(Property property) {
		if(property==null){
			throw new EcmException("级别的属性不能为空");
		}
		this.property=property;
	}
	public Property property() {
		return property;
	}
	public String levelName(){
		return property.name;
	}
	public Level nextLevel(){
		return nextLevel;
	}
	public Level nextLevel(Level next){
		nextLevel=next;
		nextLevel.parent=this;
		return next;
	}
	public Level parent() {
		return parent;
	}
	public boolean isDataType(Class<?> clazz){
		if(property==null)return false;
		if (clazz.getName().equals(property.dataType)) {
			return true;
		} 
		return false;
	}
}
