package org.jaeyo.test_storm.vo;

public class ApacheLogVO {
	private String ip;
	private String dateTime;
	private String request;
	private String response;
	private long bytesSent;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}

	public long getBytesSent() {
		return bytesSent;
	}

	public void setBytesSent(long bytesSent) {
		this.bytesSent = bytesSent;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ip : ").append(getIp());
		sb.append(", dateTime : ").append(getDateTime());
		sb.append(", request : ").append(getRequest());
		sb.append(", response : ").append(getResponse());
		sb.append(", bytesSent : ").append(getBytesSent());
		return sb.toString();
	} //toString
} // class