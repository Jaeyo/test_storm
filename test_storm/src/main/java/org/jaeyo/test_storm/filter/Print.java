package org.jaeyo.test_storm.filter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class Print extends BaseFilter{
	private String fieldName = null;
	
	public Print(String fieldName) {
		this.fieldName = fieldName;
	} //INIT
	
	public Print(){
	} //INIT

	public boolean isKeep(TridentTuple tuple) {
		if(fieldName == null){
			System.out.println("###Print : " + tuple.toString());
			return false;
		} //if
		
		Object field = tuple.getValueByField(fieldName);
		System.out.println("###Print : " + field.toString());
		return false;
	} //isKeep
} //class