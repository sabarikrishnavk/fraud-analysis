package com.pgbde.capstone.bean;

import java.io.Serializable;

/**
 * Utility class that reads file zipCodePosId.csv and using same if two zip
 * codes are provided, it returns distances.
 *
 */

public class ZipCodeData implements Serializable {
	public double lat;
	public double lon;
	public String city;
	public String state_name;
	public String postId;

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState_name() {
		return state_name;
	}

	public void setState_name(String state_name) {
		this.state_name = state_name;
	}

	public String getPostId() {
		return postId;
	}

	public void setPostId(String postId) {
		this.postId = postId;
	}

	public ZipCodeData(double lat, double lon, String city, String state_name, String postId) {
		this.lat = lat;
		this.lon = lon;
		this.city = city;
		this.state_name = state_name;
		this.postId = postId;
	}
}
