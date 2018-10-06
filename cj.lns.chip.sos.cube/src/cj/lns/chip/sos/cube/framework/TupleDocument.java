package cj.lns.chip.sos.cube.framework;

import java.util.HashMap;
import java.util.Map;

import cj.studio.ecm.EcmException;

public class TupleDocument<T> implements IDocument<T> {
	Map<String, Coordinate> coordinates;
	 T tulple;
	 String id;
	 boolean isPendding;
	  TupleDocument() {
		  coordinates=new HashMap<>();
	}
	public TupleDocument(T tuple) {
		coordinates=new HashMap<>();
		this.tulple=tuple;
	}
	@Override
	public String docid() {
		return id;
	}
	@Override
	public Coordinate coordinate(String dim) {
		return coordinates.get(dim);
	}
	@Override
	public String[] enumCoordinate() {
		return coordinates.keySet().toArray(new String[0]);
	}

	@Override
	public void addCoordinate(String dim, Coordinate coordinate) {
		if(coordinates.containsKey(dim)){
			throw new EcmException("文档中已存在维度上的坐标："+dim);
		}
		coordinates.put(dim, coordinate);
	}

	@Override
	public boolean existsCoordinate(String dim) {
		return coordinates.containsKey(dim);
	}

	@Override
	public void removeCoordinate(String dim) {
		coordinates.remove(dim);
	}
	@Override
	public T tuple() {
		return tulple;
	}
	@Override
	public boolean isPendding() {
		// TODO Auto-generated method stub
		return false;
	}


}
