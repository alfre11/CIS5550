package cis5550.kvs;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.put;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import cis5550.tools.KeyEncoder;

public class Worker extends cis5550.generic.Worker {

    private static final ConcurrentMap<String, ConcurrentMap<String, Row>> kvStore = new ConcurrentHashMap<>();
    private static String storageDirectory;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        storageDirectory = args[1];
        String coordinator = args[2];

        port(port);
        String workerID = getWorkerID(storageDirectory);
        startPingThread(coordinator, port, workerID);

        put("/data/:table/:row/:col", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");
            String colKey = req.params("col");

            String ifColumn = req.queryParams("ifcolumn");
            String equalsValue = req.queryParams("equals");
            if (ifColumn != null && equalsValue != null) {
                byte[] ifColumnValue = getRow(table, rowKey).getBytes(ifColumn);
                if (ifColumnValue == null || !new String(ifColumnValue).equals(equalsValue)) {
                    return "FAIL";
                }
            }

            putRow(table, rowKey, colKey, req.bodyAsBytes());

            Row row = getRow(table, rowKey);
            res.header("Version", String.valueOf(row.getVersion(colKey)));
            return "OK";
        });

        put("/data/:table/", (req, res) -> {
            String table = req.params("table");

            ByteArrayInputStream bais = new ByteArrayInputStream(req.bodyAsBytes());

            while (true) {
                Row rowIn = Row.readFrom(bais);
                if (rowIn == null) {
                    return "OK";
                }

                putRow(table, rowIn);
            }
        });

        get("/data/:table/:row/:col", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");
            String colKey = req.params("col");
            String versionParam = req.queryParams("version");

            Row row = getRow(table, rowKey);
            if (row == null || !row.hasColumn(colKey)) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            byte[] value;
            if (versionParam != null) {
                int version = Integer.parseInt(versionParam);
                value = row.getBytesVersion(colKey, version);
                if (value == null) {
                    res.status(404, "Not Found");
                    return "Version Not Found";
                }
            } else {
                value = row.getBytes(colKey);
            }

            res.header("Version", String.valueOf(row.getVersion(colKey)));
            res.bodyAsBytes(value);
            return null;
        });

        get("/data/:table/:row", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");

            Row row = getRow(table, rowKey);
            if (row == null) {
                res.status(404, "Row not found");
                return "Row not found";
            }

            res.header("Content-Type", "application/octet-stream");
            res.bodyAsBytes(row.toByteArray());
            return null;
        });

        get("/data/:table", (req, res) -> {
            String table = req.params("table");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            Iterator<Map.Entry<String, Row>> iterator = getTableIterator(table, startRow, endRowExclusive);
            if (iterator == null) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            res.header("Content-Type", "text/plain");

            while (iterator.hasNext()) {
                Map.Entry<String, Row> entry = iterator.next();
                res.write(entry.getValue().toByteArray());
                res.write("\n".getBytes());
            }

            res.write("\n".getBytes());
            return null;
        });

        put("/rename/:table", (req, res) -> {
            String oldTableName = req.params("table");
            String newTableName = req.body();

            boolean isOldPersistent = oldTableName.startsWith("pt-");
            boolean isNewPersistent = newTableName.startsWith("pt-");

            if (isOldPersistent == isNewPersistent) {
                // Same type renaming
                if (kvStore.containsKey(oldTableName)) {
                    if (kvStore.containsKey(newTableName)) {
                        res.status(409, "Conflict");
                        return "Table with the new name already exists";
                    }
                    ConcurrentMap<String, Row> tableData = kvStore.remove(oldTableName);
                    kvStore.put(newTableName, tableData);
                    return "OK";
                }

                File oldTableDir = new File(storageDirectory, oldTableName);
                File newTableDir = new File(storageDirectory, newTableName);

                if (oldTableDir.exists()) {
                    if (newTableDir.exists()) {
                        res.status(409, "Conflict");
                        return "Persistent table with the new name already exists";
                    }

                    boolean success = oldTableDir.renameTo(newTableDir);
                    if (success) {
                        return "OK";
                    } else {
                        res.status(500, "Internal Server Error");
                        return "Failed to rename persistent table";
                    }
                }

                res.status(404, "Not Found");
                return "Table not found";
            } else {
                if (isOldPersistent) {
                    File oldTableDir = new File(storageDirectory, oldTableName);
                    if (!oldTableDir.exists()) {
                        res.status(404, "Not Found");
                        return "Persistent table not found";
                    }
                    if (kvStore.containsKey(newTableName)) {
                        res.status(409, "Conflict");
                        return "In-memory table with the new name already exists";
                    }

                    ConcurrentMap<String, Row> newTableData = new ConcurrentHashMap<>();
                    for (File rowFile : oldTableDir.listFiles()) {
                        String rowKey = KeyEncoder.decode(rowFile.getName());
                        Row row = readRowFromDisk(oldTableName, rowKey);
                        if (row != null) {
                            for (String colKey : row.columns()) {
                                byte[] value = row.getBytes(colKey);
                                newTableData.computeIfAbsent(rowKey, k -> new Row(k)).put(colKey, value);
                            }
                        }
                        rowFile.delete();
                    }
                    oldTableDir.delete();
                    kvStore.put(newTableName, newTableData);
                    return "OK";
                } else {
                    // In-memory to persistent
                    if (!kvStore.containsKey(oldTableName)) {
                        res.status(404, "Not Found");
                        return "In-memory table not found";
                    }
                    File newTableDir = new File(storageDirectory, newTableName);
                    if (newTableDir.exists()) {
                        res.status(409, "Conflict");
                        return "Persistent table with the new name already exists";
                    }

                    newTableDir.mkdir();
                    ConcurrentMap<String, Row> oldTableData = kvStore.remove(oldTableName);
                    for (Map.Entry<String, Row> entry : oldTableData.entrySet()) {
                        String rowKey = entry.getKey();
                        Row row = entry.getValue();
                        for (String colKey : row.columns()) {
                            byte[] value = row.getBytes(colKey);
                            persistRowToDisk(newTableName, rowKey, colKey, value);
                        }
                    }
                    return "OK";
                }
            }
        });



        put("/delete/:table", (req, res) -> {
            String tableName = req.params("table");
            if (tableName.startsWith("pt-")) {
                File tableDir = new File(storageDirectory, tableName);

                if (!tableDir.exists()) {
                    res.status(404, "Not Found");
                    return "Table not found";
                }

                for (File file : tableDir.listFiles()) {
                    file.delete();
                }
                tableDir.delete();
                return "OK";
            } else {
                if (kvStore.remove(tableName) != null) return "OK";
                else {
                    res.status(404, "Not Found");
                    return "Table not found";
                }
            }
        });

        put("/delete/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            if (tableName.startsWith("pt-")) {
                File tableDir = new File(storageDirectory, tableName);
                File file = new File(tableDir, rowName);

                if (!file.exists()) {
                    res.status(404, "Not Found");
                    return "Table not found";
                }

                file.delete();
                return "OK";
            } else {
                ConcurrentMap<String, Row> tableData = kvStore.get(tableName);
                if (tableData.remove(rowName) != null) return "OK";
                else {
                    res.status(404, "Not Found");
                    return "Table/row not found";
                }
            }
        });

        get("/tables", (req, res) -> {
            StringBuilder result = new StringBuilder();

            Set<String> inMemoryTables = kvStore.keySet();
            for (String table : inMemoryTables) {
                result.append(table).append("\n");
            }

            File storageDir = new File(storageDirectory);
            String[] persistentTables = storageDir.list();
            if (persistentTables != null) {
                for (String table : persistentTables) {
                    if (!inMemoryTables.contains(table)) {
                        result.append(table).append("\n");
                    }
                }
            }

            res.header("Content-Type", "text/plain");
            return result.toString();
        });

        get("/count/:table", (req, res) -> {
            String tableName = req.params("table");

            if (kvStore.containsKey(tableName)) {
                int rowCount = kvStore.get(tableName).size();
                return String.valueOf(rowCount);
            }

            File tableDir = new File(storageDirectory, tableName);
            if (tableDir.exists() && tableDir.isDirectory()) {
                int rowCount = tableDir.list().length;
                return String.valueOf(rowCount);
            }

            res.status(404, "Not Found");
            return "Table not found";
        });

        get("/", (req, res) -> {
            StringBuilder html = new StringBuilder();
            html.append("<html><body><h1>Tables</h1><table border='1'><tr><th>Table Name</th><th>Row Count</th></tr>");

            Set<String> inMemoryTables = kvStore.keySet();
            for (String table : inMemoryTables) {
                int rowCount = kvStore.get(table).size();
                html.append("<tr><td><a href='/view/").append(table).append("'>").append(table).append("</a></td><td>").append(rowCount).append("</td></tr>");
            }

            File storageDir = new File(storageDirectory);
            String[] persistentTables = storageDir.list();
            if (persistentTables != null) {
                for (String table : persistentTables) {
                    if (!table.startsWith("pt-")) continue;

                    File tableDir = new File(storageDirectory, table);
                    int rowCount = tableDir.list().length;
                    html.append("<tr><td><a href='/view/").append(table).append("'>").append(table).append("</a></td><td>").append(rowCount).append("</td></tr>");
                }
            }

            html.append("</table></body></html>");

            res.header("Content-Type", "text/html");
            return html.toString();
        });

        get("/view/:table", (req, res) -> {
            String tableName = req.params("table");
            String fromRow = req.queryParams("fromRow");
            int pageSize = 50;

            StringBuilder html = new StringBuilder();
            html.append("<html><body><h1>Table: ").append(tableName).append("</h1><table border='1'><tr><th>Row Key</th>");
            // add columns after: <th>Columns</th></tr>

            if (kvStore.containsKey(tableName)) {
                ConcurrentMap<String, Row> table = kvStore.get(tableName);
                String[] sortedRows = table.keySet().toArray(new String[0]);
                Row row = table.get(sortedRows[0]);
                for (String col : row.columns()) {
                    html.append("<th>").append(col).append("</th>");
                }
                html.append("</tr>");

                boolean nextPage = false;
                int count = 0;

                for (int i = 0; i < sortedRows.length; i++) {
                    if (fromRow == null || sortedRows[i].compareTo(fromRow) > 0) {
                        row = table.get(sortedRows[i]);
                        html.append("<tr><td>").append(sortedRows[i]).append("</td>");

                        for (String col : row.columns()) {
                            html.append("<td>").append(row.get(col)).append("</td>");
                        }
                        html.append("</tr>");

                        count++;
                        if (count >= pageSize) {
                            if (i + 1 < sortedRows.length) {
                                nextPage = true;
                                fromRow = sortedRows[i];
                            }
                            break;
                        }
                    }
                }

                if (nextPage) {
                    html.append("<tr><td colspan='2'><a href='/view/").append(tableName).append("?fromRow=").append(fromRow).append("'>Next</a></td></tr>");
                }
            } else {
                File tableDir = new File(storageDirectory, tableName);
                if (tableDir.exists() && tableDir.isDirectory()) {
                    File[] files = tableDir.listFiles();

                    String rowKey = KeyEncoder.decode(files[0].getName());
                    Row row = readRowFromDisk(tableName, rowKey);
                    for (String col : row.columns()) {
                        html.append("<th>").append(col).append("</th>");
                    }
                    html.append("</tr>");

                    boolean nextPage = false;
                    int count = 0;

                    for (int i = 0; i < files.length; i++) {
                        rowKey = KeyEncoder.decode(files[i].getName());

                        if (fromRow == null || rowKey.compareTo(fromRow) > 0) {
                            row = readRowFromDisk(tableName, rowKey);
                            if (row != null) {

                                html.append("<tr><td>").append(rowKey).append("</td>");
                                for (String col : row.columns()) {
                                    html.append("<td>").append(row.get(col)).append("</td>");
                                }
                                html.append("</tr>");

                                count++;
                                if (count >= pageSize) {
                                    if (i + 1 < files.length) {
                                        nextPage = true;
                                        fromRow = rowKey;
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    if (nextPage) {
                        html.append("<tr><td colspan='2'><a href='/view/").append(tableName).append("?fromRow=").append(fromRow).append("'>Next</a></td></tr>");
                    }
                } else {
                    res.status(404, "Not Found");
                    return "Table not found";
                }
            }

            html.append("</table></body></html>");
            res.header("Content-Type", "text/html");
            return html.toString();
        });
    }

    private static Row getRow(String table, String rowKey) throws Exception {
        if (table.startsWith("pt-")) {
            return readRowFromDisk(table, rowKey);
        }
        return kvStore.getOrDefault(table, new ConcurrentHashMap<>()).get(rowKey);
    }

    private static void putRow(String table, String rowKey, String colKey, byte[] value) throws Exception {
        if (table.startsWith("pt-")) {
            persistRowToDisk(table, rowKey, colKey, value);
        } else {
            kvStore.putIfAbsent(table, new ConcurrentHashMap<>());
            Row row = kvStore.get(table).computeIfAbsent(rowKey, Row::new);
            row.put(colKey, value);
        }
    }

    private static void putRow(String table, Row rowIn) throws IOException {
        if (table.startsWith("pt-")) {
            File tableDir = new File(storageDirectory, table);
            if (!tableDir.exists()) {
                tableDir.mkdir();
            }
            File rowFile = new File(tableDir, KeyEncoder.encode(rowIn.key()));

            try (FileOutputStream fos = new FileOutputStream(rowFile)) {
                fos.write(rowIn.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            kvStore.putIfAbsent(table, new ConcurrentHashMap<>());
            kvStore.get(table).put(rowIn.key(), rowIn);
        }
    }

    private static void persistRowToDisk(String table, String rowKey, String colKey, byte[] value) throws Exception {
        File tableDir = new File(storageDirectory, table);
        if (!tableDir.exists()) {
            tableDir.mkdir();
        }

        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
        Row row;
        if (rowFile.exists()) {
            row = readRowFromDisk(table, rowKey);
        } else {
            row = new Row(rowKey);
        }
        row.put(colKey, value);

        try (FileOutputStream fos = new FileOutputStream(rowFile)) {
            fos.write(row.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Row readRowFromDisk(String table, String rowKey) throws Exception {
        File tableDir = new File(storageDirectory, table);
        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));

        if (!rowFile.exists()) {
            return null;
        }

        try (FileInputStream fis = new FileInputStream(rowFile)) {
            return Row.readFrom(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getWorkerID(String storageDir) {
        File idFile = new File(storageDir + "/id");

        try {
            if (idFile.exists()) {
                return Files.readString(idFile.toPath());
            } else {
                String id = generateRandomID();
                Files.createDirectories(idFile.toPath().getParent());
                Files.write(idFile.toPath(), id.getBytes());
                return id;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error while reading or writing worker ID: " + e.getMessage());
        }
    }

    private static Iterator<Map.Entry<String, Row>> getTableIterator(String table, String startRow, String endRowExclusive) {    	
    	if (table.startsWith("pt-")) {
            File tableDir = new File(storageDirectory, table);
            if (!tableDir.exists() || !tableDir.isDirectory()) {
                return null;
            }

            File[] rowFiles = tableDir.listFiles();
            if (rowFiles == null) {
                return null;
            }

            Arrays.sort(rowFiles, (f1, f2) -> {
                try {
                    String rowKey1 = KeyEncoder.decode(f1.getName());
                    String rowKey2 = KeyEncoder.decode(f2.getName());
                    return rowKey1.compareTo(rowKey2);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            return Arrays.stream(rowFiles)
                    .map(file -> {
                        try {
                            String rowKey = KeyEncoder.decode(file.getName());
                            return Map.entry(rowKey, readRowFromDisk(table, rowKey));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(entry -> (startRow == null || entry.getKey().compareTo(startRow) >= 0)
                            && (endRowExclusive == null || entry.getKey().compareTo(endRowExclusive) < 0))
                    .iterator();
        }
    	
    	ConcurrentMap<String, Row> rows = kvStore.get(table);
        if (rows == null) {
            return null;
        }

        return rows.entrySet().stream().filter(entry -> (startRow == null || entry.getKey().compareTo(startRow) >= 0) && (endRowExclusive == null || entry.getKey().compareTo(endRowExclusive) < 0)).iterator();
    }
}
