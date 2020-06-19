package com.want.bigdata.service;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "want.jco")
public class JcoConfig {

	private String  host;
	private String  system;
	private String  client;
	private String  username;
	private String  password;
	private String  lang;
	private int maxtime;
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getSystem() {
		return system;
	}
	public void setSystem(String system) {
		this.system = system;
	}
	public String getClient() {
		return client;
	}
	public void setClient(String client) {
		this.client = client;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public int getMaxtime() {
		return maxtime;
	}
	public void setMaxtime(int maxtime) {
		this.maxtime = maxtime;
	}
	@Override
	public String toString() {
		return "JcoConfig [host=" + host + ", system=" + system + ", client=" + client + ", username=" + username
				+ ", password=" + password + ", lang=" + lang + ", maxtime=" + maxtime + "]";
	}

	
}
