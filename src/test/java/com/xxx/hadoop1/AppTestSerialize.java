package com.xxx.hadoop1;

import java.io.Serializable;

public class AppTestSerialize implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1722361449057543134L;
	private String name;
	
	public String getName() {
		return name;
	}

	public AppTestSerialize(String name) {
		super();
		this.name = name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
