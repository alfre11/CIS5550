package cis5550.kvs;

import java.util.*;
import java.io.*;

public class Row implements Serializable {

    protected String key;
    protected HashMap<String, TreeMap<Integer, byte[]>> values;
    protected HashMap<String, Integer> latestVersionMap;

    public Row(String keyArg) {
        key = keyArg;
        values = new HashMap<>();
        latestVersionMap = new HashMap<>();
    }

    public synchronized String key() {
        return key;
    }

    public synchronized Row clone() {
        Row theClone = new Row(key);
        for (String col : values.keySet()) {
            theClone.values.put(col, new TreeMap<>(values.get(col)));
        }
        for (String col : latestVersionMap.keySet()) {
            theClone.latestVersionMap.put(col, latestVersionMap.get(col));
        }
        return theClone;
    }

    public synchronized Set<String> columns() {
        return values.keySet();
    }

    public synchronized void put(String colKey, byte[] value) {
        int newVersion = latestVersionMap.getOrDefault(colKey, 0) + 1;
        latestVersionMap.put(colKey, newVersion);

        values.putIfAbsent(colKey, new TreeMap<>());
        values.get(colKey).put(newVersion, value);
    }

    public synchronized void put(String colKey, String value) {
        put(colKey, value.getBytes());
    }

    public synchronized String get(String colKey) {
        if (!values.containsKey(colKey))
            return null;
        int latestVersion = latestVersionMap.getOrDefault(colKey, 0);
        return new String(values.get(colKey).get(latestVersion));
    }

    public synchronized byte[] getBytes(String colKey) {
        if (!values.containsKey(colKey))
            return null;
        int latestVersion = latestVersionMap.getOrDefault(colKey, 0);
        return values.get(colKey).get(latestVersion);
    }

    public synchronized byte[] getBytesVersion(String colKey, int version) {
        if (!values.containsKey(colKey) || !values.get(colKey).containsKey(version))
            return null;
        return values.get(colKey).get(version);
    }

    public synchronized int getVersion(String colKey) {
        return latestVersionMap.getOrDefault(colKey, 0);
    }

    public synchronized boolean hasColumn(String colKey) {
        return values.containsKey(colKey);
    }

    public synchronized byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            baos.write(key.getBytes());
            baos.write(' ');

            for (String col : values.keySet()) {
                baos.write(col.getBytes());
                baos.write(' ');
                baos.write((""+values.get(col).get(latestVersionMap.get(col)).length).getBytes());
                baos.write(' ');
                baos.write(values.get(col).get(latestVersionMap.get(col)));
                baos.write(' ');
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("This should not happen!");
        }

        return baos.toByteArray();
    }

    public static Row readFrom(InputStream in) throws Exception {
        String theKey = readStringSpace(in);
        if (theKey == null)
            return null;

        Row newRow = new Row(theKey);
        while (true) {
            String keyOrMarker = readStringSpace(in);
            if (keyOrMarker == null)
                return newRow;

            int len = Integer.parseInt(readStringSpace(in));
            byte[] theValue = new byte[len];
            int bytesRead = 0;
            while (bytesRead < len) {
                int n = in.read(theValue, bytesRead, len - bytesRead);
                if (n < 0)
                    throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read " + bytesRead + " bytes, expecting " + len + ")");
                bytesRead += n;
            }

            byte b = (byte) in.read();
            if (b != ' ')
                throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");

            newRow.put(keyOrMarker, theValue);
        }
    }

    static String readStringSpace(InputStream in) throws Exception {
        byte buffer[] = new byte[16384];
        int numRead = 0;
        while (true) {
            if (numRead == buffer.length)
                throw new Exception("Format error: Expecting string+space");

            int b = in.read();
            if ((b < 0) || (b == 10))
                return null;
            buffer[numRead++] = (byte)b;
            if (b == ' ')
                return new String(buffer, 0, numRead - 1);
        }
    }

    public static Row readFrom(RandomAccessFile in) throws Exception {
        String theKey = readStringSpace(in);
        if (theKey == null)
            return null;

        Row newRow = new Row(theKey);
        while (true) {
            String keyOrMarker = readStringSpace(in);
            if (keyOrMarker == null)
                return newRow;

            int len = Integer.parseInt(readStringSpace(in));
            byte[] theValue = new byte[len];
            int bytesRead = 0;
            while (bytesRead < len) {
                int n = in.read(theValue, bytesRead, len - bytesRead);
                if (n < 0)
                    throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read " + bytesRead + " bytes, expecting " + len + ")");
                bytesRead += n;
            }

            byte b = (byte) in.read();
            if (b != ' ')
                throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");

            newRow.put(keyOrMarker, theValue);
        }
    }

    static String readStringSpace(RandomAccessFile in) throws Exception {
        byte buffer[] = new byte[16384];
        int numRead = 0;
        while (true) {
            if (numRead == buffer.length)
                throw new Exception("Format error: Expecting string+space");

            int b = in.read();
            if ((b < 0) || (b == 10))
                return null;
            buffer[numRead++] = (byte)b;
            if (b == ' ')
                return new String(buffer, 0, numRead - 1);
        }
    }

    public synchronized String toString() {
        String s = key + " {";
        boolean isFirst = true;
        for (String col : values.keySet()) {
            s = s + (isFirst ? " " : ", ") + col + ": " + new String(values.get(col).lastEntry().getValue());
            isFirst = false;
        }
        return s + " }";
    }
}
