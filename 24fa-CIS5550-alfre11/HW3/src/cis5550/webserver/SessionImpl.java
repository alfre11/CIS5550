package cis5550.webserver;

import java.util.*;

import cis5550.tools.Logger;

import java.security.SecureRandom;

public class SessionImpl implements Session{
	
	String sessionID;
	long creationTime;
	long lastAccessed;
	int maxAccessInterval;
	Map<String, Object> attributeMap;
	private boolean valid;
	private static SecureRandom random = new SecureRandom();
	private static final String CHAR_SET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	private static final Logger logger = Logger.getLogger(Server.class);
	
	public SessionImpl() {
		StringBuilder sessionId = new StringBuilder(20);
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(CHAR_SET.length());
            sessionId.append(CHAR_SET.charAt(index));
        }
        sessionID = sessionId.toString();
		creationTime = System.currentTimeMillis();
		this.lastAccessed = System.currentTimeMillis();
		attributeMap = new HashMap<String, Object>();
		valid = true;
		maxAccessInterval = 300;
	}
	
	
	@Override
	public String id() {
		return sessionID;
	}

	@Override
	public long creationTime() {
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return lastAccessed;
	}
	
	public void setLastAccessed(long accessTime) {
		if(!isExpired()) {
			lastAccessed = accessTime;
		}
	}

	@Override
	public void maxActiveInterval(int seconds) {
		maxAccessInterval = seconds;
	}
	
	public long maxInterval() {
		return maxAccessInterval;
	}

	@Override
	public void invalidate() {
		valid = false;
	}
	
	public boolean isValid() {
		return valid;
	}

	@Override
	public Object attribute(String name) {
		return attributeMap.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		attributeMap.put(name, value);
	}
	
	public boolean isExpired() {
		logger.info("current time: " + Long.toString(System.currentTimeMillis()));
		logger.info("last accessed: " + Long.toString(lastAccessed));
		logger.info("max interval: " + maxAccessInterval);
		return (System.currentTimeMillis() - lastAccessed > maxAccessInterval * 1000);
	}

	
}
