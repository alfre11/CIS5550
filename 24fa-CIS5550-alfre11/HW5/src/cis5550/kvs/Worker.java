package cis5550.kvs;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import cis5550.webserver.Server;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;
import cis5550.kvs.KVSClient.*;

public class Worker extends cis5550.generic.Worker {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static Map<String, Map<String, Row>> tables = new HashMap<>();
//    private static Map<String, List<String>> diskRowKeys = new HashMap<>();
//    private static final String DISK_STORAGE = "./data";
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: Worker <port> <storageDir> <coordinatorIP:port>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        System.out.println("Worker run on port: " + port);
        String storageDir = args[1];
        String coordinatorURL = "http://" + args[2];

        File idFile = new File(storageDir + "/id");
        String workerID;
        if (idFile.exists()) {
            workerID = new String(Files.readAllBytes(idFile.toPath())).trim();
        } else {
            workerID = generateRandomID();
            Files.write(idFile.toPath(), workerID.getBytes());
        }


        cis5550.generic.Worker.startPingThread(workerID, port, coordinatorURL);

        Server.port(port);
        Server.put("/data/:table/:row/:column", (req, res) -> {
            String table = req.params("table");
            String row = req.params("row");
            String column = req.params("column");
            byte[] data = req.bodyAsBytes();

            putRow(table, row, column, data, storageDir);
            return "OK";
        });

        Server.get("/data/:table/:row/:column", (req, res) -> {
            String table = req.params("table");
            String row = req.params("row");
            String column = req.params("column");

            String data = getRow(table, row, column, storageDir);
            if (data == null) {
                res.status(404, "Not Found");
            } else {
                res.body(data);
            }
            return null;
        });
        
        Server.get("/", (request, response) -> {
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>Key-Value Store Tables</title></head><body>");
            html.append("<h1>List of Tables</h1>");
            html.append("<table border='1'>");
            html.append("<tr><th>Table Name</th><th>Number of Rows</th></tr>");

            for (String tableName : tables.keySet()) {
                Map<String, Row> table = tables.get(tableName);
                int rowCount = table.size();
                html.append("<tr>");
                html.append("<td><a href='/view/").append(tableName).append("'>").append(tableName).append("</a></td>");
                html.append("<td>").append(rowCount).append("</td>");
                html.append("</tr>");
            }

            html.append("</table>");
            html.append("</body></html>");

            response.header("Content-Type", "text/html");
            response.body(html.toString());
            return null;
        });
        
        Server.get("/view/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String fromRow = req.queryParams("fromRow");
            Map<String, Row> table = tables.get(tableName);
            boolean moreRows = false;
            if (table == null) {
                res.status(404, "Table not found");
                return "Table not found";
            }
            
            List<String> sortedRowKeys = new ArrayList<>(table.keySet());
            Collections.sort(sortedRowKeys);
            int startIndex = 0;
            
            if (fromRow != null && !fromRow.isEmpty()) {
                for (int i = 0; i < sortedRowKeys.size(); i++) {
                    if (sortedRowKeys.get(i).compareTo(fromRow) >= 0) {
                    	startIndex = i;
                        break;
                    }
                }
            }
            
            List<String> paginatedRows = new ArrayList<>();
            for (int i = startIndex; i < Math.min(startIndex + 10, sortedRowKeys.size()); i++) {
                paginatedRows.add(sortedRowKeys.get(i));
            }
            if (startIndex + 10 < sortedRowKeys.size()) {
                moreRows = true;
            }

            res.type("text/html");
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>Table View: ").append(tableName).append("</title></head><body>");
            html.append("<h1>Table: ").append(tableName).append("</h1>");

            Set<String> columnNames = new TreeSet<>();
            for (String rowName : paginatedRows) {
                columnNames.addAll(table.get(rowName).columns());
            }
            for (String rowName : table.keySet()) {
                columnNames.addAll(table.get(rowName).columns());
            }

            //building the table
            html.append("<table border='1'><tr><th>Row Key</th>");
            for (String colName : columnNames) {
                html.append("<th>").append(colName).append("</th>");
            }
            html.append("</tr>");

            //Add rows
            for (String rowName : paginatedRows) {
            	Row currRow = table.get(rowName);
                html.append("<tr><td>").append(rowName).append("</td>");
                for (String colName : columnNames) {
                    String cellValue = currRow.get(colName);
                    html.append("<td>").append(cellValue != null ? cellValue : "").append("</td>");
                }
                html.append("</tr>");
            }
            html.append("</table>");
            if (moreRows) {
                String nextRow = sortedRowKeys.get(startIndex + 10);  // The key for the next row
                html.append("<a href=\"/view/").append(tableName).append("?fromRow=").append(nextRow).append("\">Next</a>");
            }
            html.append("</body></html>");

            return html.toString();
        });
        
        Server.get("/data/:tableName/:rowKey", (req, res) -> {
            String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");
            Row row = null;
            
            if(tableName.startsWith("pt-")) {
            	File tabDir = new File(storageDir, tableName);
            	if(tabDir.exists()) {
            		String codedKey = KeyEncoder.decode(rowKey);
            		File rowFile = new File(tabDir, codedKey);
            		if(rowFile.exists()) {
            			row = Row.readFrom(new FileInputStream(rowFile));
            		} else {
            			res.status(404, "Not Found");
            			return "Row not Found";
            		}
            	} else {
            		res.status(404, "Not Found");
            		return "Table Not Found";
            	}
            } else {
                Map<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    return "Table not found";
                }

                row = table.get(rowKey);
                if (row == null) {
                    res.status(404, "Not Found");
                    return "Row not found";
                }
            }


            byte[] rowData = row.toByteArray();
            
            res.type("application/octet-stream");
            
            res.write(rowData);
            return "OK";
        });
        
        Server.get("/data/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            byte[] rowData = null;
            
            
            if(tableName.startsWith("pt-")) {
            	File tabDir = new File(storageDir, tableName);
            	if(tabDir.exists()) {
            		File[] rows = tabDir.listFiles();
            		for(File rowFile : rows) {
            			Row r = Row.readFrom(new FileInputStream(rowFile));
            			res.write(r.toByteArray());
            			res.write("\n".getBytes());
            		}
            		res.write("\n".getBytes());
            	} else {
            		res.status(404, "Not Found");
            		return "Not Found";
            	}
            } else {
            	 Map<String, Row> table = tables.get(tableName);
                 if (table == null) {
                     res.status(404, "Not Found");
                     return "Table not found";
                 }

                 // Sort row keys
                 List<String> sortedRowKeys = new ArrayList<>(table.keySet());
//                 Collections.sort(sortedRowKeys);

                 // Filter rows based on startRow and endRowExclusive
                 List<String> filteredRows = new ArrayList<>();
                 for (String rowKey : sortedRowKeys) {
                     if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
                         (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                         filteredRows.add(rowKey);
                     }
                 }

                 res.type("text/plain");

                 for (String rowKey : filteredRows) {
                     Row row = table.get(rowKey);
                     rowData = row.toByteArray();
                     res.write(rowData);
                     res.write("\n".getBytes());
                 }
                 res.write("\n".getBytes());
            }
            return "OK";
        });        
        
        Server.get("/count/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            
            if(tableName.startsWith("pt-")) {
            	File tabFile = new File(storageDir, tableName);
            	if(tabFile.exists()) {
            		File[] rows = tabFile.listFiles();
            		return rows.length;
            	}
            } else {
            	//in-memory table
            	Map<String, Row> table = tables.get(tableName);
            	if (table != null) {
            		res.type("text/plain");
            		return String.valueOf(table.size());
            	}
            }
            res.status(404, "Not Found");
            return "table not found";
        });
        
        Server.put("/rename/:oldTableName", (req, res) -> {
            String oldTableName = req.params("oldTableName");
            String newTableName = req.body();

            if (tables.containsKey(oldTableName)) {
                Map<String, Row> table = tables.remove(oldTableName);
                if(table == null) {
                	res.status(404, "Not Found");
                	return "table not found";
                }
                if(newTableName.startsWith("pt-")) {
                	File newTabFile = new File(storageDir, newTableName);
                	if(newTabFile.exists()) {
                        res.status(409, "Table Already Exists");
                        return "Table Already Exists";
                	}
                	
                	for(String rowName : table.keySet()) {
                		Row curr = table.get(rowName);
                		File rowFile = new File(newTabFile, rowName);
                		try (FileOutputStream fos = new FileOutputStream(rowFile)) {
                        	byte[] out = curr.toByteArray();
                            fos.write(out);
                            return "OK";
                        } catch (FileNotFoundException e) {
                			e.printStackTrace();
                			logger.error("could not find file");
                		} catch (IOException e) {
                			e.printStackTrace();
                		}
                	}
                } else {
                    tables.put(newTableName, table);
                    return "OK";	
                }
            }

            //persistent table, rename directory on disk
            if(oldTableName.startsWith("pt-")) {
                File oldTabFile = new File(storageDir, oldTableName);
                if(oldTabFile.exists()) {
                    File newTabFile = new File(storageDir, newTableName);
                    if(newTabFile.exists()) {
                    	res.status(409, "Table Already Exists");
                    	return "Table Already Exists";
                    }
                    if(!newTableName.startsWith("pt-")) {
                    	res.status(400, "Bad Request");
                    	return "Bad Request";
                    }
                    newTabFile.mkdir();
                    Files.move(oldTabFile.toPath(), newTabFile.toPath());
                }
            }

            res.status(404, "Not Found");
            return "Table not found";
            
        });
        
        Server.put("/delete/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if(tableName.startsWith("pt-")) {
                File tabFile = new File(storageDir, tableName);
                if(!tabFile.exists()) {
                	res.status(404, "Not Found");
                    return "Table not found";
                }
                File[] rows = tabFile.listFiles();
                for(File row : rows) {
                	row.delete();
                }
                tabFile.delete();
            } else {
                //table exists in memory
                if (tables.containsKey(tableName)) {
                    //Remove in-memory table
                    tables.remove(tableName);
                } else {
                	res.status(404, "Not Found");
                    return "Table not found";
                }
            }
            return "OK";
        });
        
        Server.get("/tables", (req, res) -> {
            StringBuilder responseText = new StringBuilder();
            for (String tableName : tables.keySet()) {
                responseText.append(tableName).append("\n");
            }
            File storFile = new File(storageDir);
            File[] directories = storFile.listFiles(File::isDirectory);
            
            if (directories != null) {
                for (File directory : directories) {
                    responseText.append(directory.getName()).append("\n");
                }
            }
            res.type("text/plain");
            return responseText.toString();
        });


    }

    private static String generateRandomID() {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return id.toString();
    }

    private static void putRow(String table, String row, String column, byte[] data, String dir) {
    	//check for pt-
    	if(table.startsWith("pt-")) {
    		writeToDisk(table, row, dir, column, data);
    	} else {
            tables.computeIfAbsent(table, k -> new HashMap<>());
            Row r = tables.get(table).computeIfAbsent(row, k -> new Row(k));
            r.put(column, data);
    	}
    }

    private static String getRow(String table, String row, String column, String dir) {
    	Row r = null;
    	if(table.startsWith("pt-")) {
    		File tabFile = new File(dir, table);
    		if(tabFile.exists()) {
    			String codedKey = KeyEncoder.decode(row);
    			File rowFile = new File(tabFile, codedKey);
    			if(rowFile.exists()) {
    				try {
						r = Row.readFrom(new FileInputStream(rowFile));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			} else {
    				return null;
    			}
    		} else {
    			logger.info("no such table");
    			return null;
    		}
    	} else {
            if (!tables.containsKey(table)) return null;
            r = tables.get(table).get(row);
            if (r == null) return null;
            
    	}
    	return r.get(column);
    }
    
    private static void writeToDisk(String table, String row, String dir, String col, byte[] data) {
        File tableDir = new File(dir, table);
        if(!tableDir.exists()) {
        	tableDir.mkdir();
        }
        
        String codedKey = KeyEncoder.encode(row);
        
        File rowFile = new File(tableDir, codedKey);
        Row r = null;
        if(rowFile.exists()) {
        	try {
				r = Row.readFrom(new FileInputStream(rowFile));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } else {
        	r = new Row(row);
        }
        r.put(col, data);
        
        try (FileOutputStream fos = new FileOutputStream(rowFile)) {
        	byte[] out = r.toByteArray();
            fos.write(out);
        } catch (FileNotFoundException e) {
			e.printStackTrace();
			logger.error("could not find file");
		} catch (IOException e) {
			e.printStackTrace();
		}
        
    }
}

