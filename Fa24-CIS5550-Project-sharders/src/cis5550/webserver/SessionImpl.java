package cis5550.webserver;

import java.util.HashMap;
import java.security.SecureRandom;

public class SessionImpl implements Session {
    private String sessionId = "";
    private long creationTime;
    private long lastAccessedTime;
    private int maxTime = 300;
    private HashMap<String, Object> attributes = new HashMap<>();

    private final String letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    private final int numLetters = letters.length();
    private final SecureRandom random = new SecureRandom();

    SessionImpl() {
        creationTime = System.currentTimeMillis();
        lastAccessedTime = creationTime;
        createID();
    }

    private void createID() {
        int chars = 20;
        for (int i = 0; i < chars; i++) {
            sessionId += letters.charAt(random.nextInt(numLetters));
        }
    }

    @Override
    public String id() {
        return sessionId;
    }

    public void setId(String id) {
        this.sessionId = id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    public void updateTime() {
        this.lastAccessedTime = System.currentTimeMillis();
    }

    @Override
    public void maxActiveInterval(int seconds) {
        maxTime = seconds;
    }

    public int getMaxActiveInterval() {
        return maxTime;
    }

    @Override
    public void invalidate() {
        Server.removeSession(sessionId);
    }

    @Override
    public Object attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }
}
