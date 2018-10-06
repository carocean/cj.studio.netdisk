package cj.lns.chip.sos.cube.framework;

import java.util.ArrayList;
import java.util.List;

import cj.studio.ecm.EcmException;

public class Dimension {
	String name;
	String alias;
	String desc;
	Hierarcky hierarcky;
	public String id;

	public Dimension() {

	}
	public String phyId() {
		return id;
	}
	public Dimension(String name) {
		this.name=name;
	}
	public String getName() {
		return name;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public void setName(String name) {
		this.name = name;
	}
	public String[] porperties(){
		List<String> list=new ArrayList<>();
		Hierarcky hier=hierarcky();
		Level level=hier.head;
		do{
			if(level.property==null){
				continue;
			}
			list.add(level.property.name);
		}while(level!=null);
		return list.toArray(new String[0]);
	}
	public Hierarcky hierarcky() {
		if (hierarcky == null) {
			if(name==null){
				throw new EcmException("维度未有命名");
			}
			hierarcky=new Hierarcky(name);
		}
		return hierarcky;
	}

	public String getAlias() {
		return alias;
	}
	
	public String getDesc() {
		return desc;
	}

	public String toBson() {
		StringBuffer sb=new StringBuffer();
		sb.append("{");
		sb.append("'_dimension':{");
		sb.append(String.format("'alias':'%s',",alias));
		sb.append(String.format("'name':'%s',",name));
		sb.append(String.format("'desc':'%s'",desc));
		
		sb.append("},");//end dim desc
		Level level=hierarcky().head;
		while(level!=null){
			sb.append(String.format("'%s':{",level.property.name));
			sb.append(String.format("'alias':'%s',",level.property.alias));
			sb.append(String.format("'dataType':'%s'",level.property.dataType));
			sb.append("},");
			level=level.nextLevel();
		}
		if(!hierarcky().isEmpty()){
			sb.deleteCharAt(sb.length()-1);
		}
		
		sb.append("}");//
		return sb.toString();
	}

}
