package cis5550.webserver;

import cis5550.tools.Logger;

import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.text.SimpleDateFormat;

import static java.net.URLDecoder.decode;

public class ChildThread extends Thread {

    private static final Logger logger = Logger.getLogger(ChildThread.class);
    private final Socket socket;
    private final String rootPath;

    private Boolean isHEAD = false;

    private Date headerDate;
    private Boolean isIfModSinceFlag = false;

    private Boolean isRangeFlag = false;
    private int start = 0;
    private int end = Integer.MAX_VALUE;

    private final HashMap<String, Route> routes;
    private Map<String, String> paramsMap = null;
    private Map<String, String> queryMap = null;
    private Map<String, String> cookiesMap = null;
    private SessionImpl sessionCookie = null;
    private String sessionID = null;
    private final Server server;

    /**
     * Constructor for a ChildThread object, which is a thread to accept incoming connections to a ServerSocket
     *
     * @param entrySocket Socket to create a parallel thread for
     * @param ag1         Path, from args[1] of the ServerSocket, which specifies the parent directory of file retrieval
     */
    public ChildThread(Socket entrySocket, String ag1, HashMap<String, Route> serverRoutes, Server serverIn) {
        socket = entrySocket;
        rootPath = ag1;
        routes = serverRoutes;
        server = serverIn;
    }

    @Override
    public void run() {
        // Try loop to throw I/O exceptions
//        logger.debug("enter run for child thread");
        try {
            // We want a new PrintWriter for each new socket
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            logger.debug("");
            logger.debug("");
            logger.debug("Connection from: " + socket.getRemoteSocketAddress());

            // We want a while true so we keep a persistent connection to the Socket and only drop it when the client
            // closes it first
            while (true) {

                // Byte-by-Byte parse headers
                ByteArrayOutputStream byteHeader = new ByteArrayOutputStream();
                InputStream inputStream = socket.getInputStream();
                int inputByte;
                int step = 0;
                boolean foundHeader = false;
                // Re-init maps each time
                paramsMap = new HashMap<>();
                queryMap = new HashMap<>();
                cookiesMap = new HashMap<>();
                sessionCookie = null;
                sessionID = null;

                // Find CLRF bytes
                while ((inputByte = inputStream.read()) != -1) {
                    byteHeader.write(inputByte);
                    // Pattern match binary of /r/n
                    switch (step) {
                        case 0: // default
                            if (inputByte == 13) {
                                step = 1;
                            }
                            break;
                        case 1:
                            if (inputByte == 10) {
                                step = 2;
                            } else {
                                step = 0;
                            }
                            break;
                        case 2:
                            if (inputByte == 13) {
                                step = 3;
                            } else {
                                step = 0;
                            }
                            break;
                        case 3:
                            if (inputByte == 10) {
                                // step = 4;
                                foundHeader = true;
                                //logger.debug("Found header through byte-reading");
                            } else {
                                step = 0;
                            }
                            break;
                    }
                    // Exit the while loop once we are able to reach state = 3
                    if (foundHeader) {
                        break;
                    }
                }

                // If the inputByte reads the end of the input stream first
                if (inputByte == -1) {
                    break;
                }

                // Nested reader object creation based on directions
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new ByteArrayInputStream(byteHeader.toByteArray())));

                String line;
                List<String> messageContent = new ArrayList<>();
                int contentLength = 0;
                String contentType = "";

                // We run a BufferedReader over the bytes we read in to parse the header and its details
                while ((line = reader.readLine()) != null) {
                    logger.debug(line); // Expecting an empty line at the end?
                    //System.out.println(line);
                    messageContent.add(line);

                    // Extract the content length
                    if (line.toLowerCase().contains("content-length")) {
                        contentLength = Integer.parseInt(line.split(": ")[1]);
                    }

                    // Extract the content type
                    if (line.toLowerCase().contains("content-type")) {
                        contentType = line.split(": ")[1];
                    }

                    // Check if the request is "HEAD" and update the global flag
                    if (line.contains("HEAD")) {
                        isHEAD = true;
                    }

                    // Check for cookie
                    if (line.toLowerCase().contains("cookie: ")) {
                		String[] tempHeader = new String[2];
                    	try {
                    		tempHeader = line.split(": ")[1].split(";");
                    	} catch (Exception e) {
                    		System.out.println(line);
                    	}
                        for (String t : tempHeader) {
                            String[] cookie = t.split("=");
                            cookiesMap.put(cookie[0], cookie[1]);
                            if (cookie[0].toLowerCase().equals("sessionid")) {
                                sessionCookie = Server.getSession(cookie[1]);
                                if ((sessionCookie != null) && (System.currentTimeMillis() - sessionCookie.lastAccessedTime()) < sessionCookie.getMaxActiveInterval() * 1000L) {
                                    logger.debug("Found an existing session: " + sessionCookie.id());
                                    sessionCookie.updateTime();
                                } else if ((System.currentTimeMillis() - sessionCookie.lastAccessedTime()) >= sessionCookie.getMaxActiveInterval() * 1000L) {
                                    logger.debug("Invalidated a session during request read-in: " + sessionCookie.id());
                                    sessionCookie.invalidate();
                                    sessionCookie = null;
                                }
                            }
                        }
                    }

                    //EC check for If-Modified-Since
                    if (line.toLowerCase().contains("if-modified-since:")) {
                        // Parse out the date object
                        String stringDate = line.split(": ")[1];
                        SimpleDateFormat rfc1123DateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
                        rfc1123DateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                        headerDate = rfc1123DateFormat.parse(stringDate);
                        Date currentDate = new Date();
                        if (currentDate.before(headerDate)) {
                            sendErrorResponse(out, 400, "Bad Request", isHEAD);
                        } else {
                            isIfModSinceFlag = true;
                        }
                    }

                    //EC check for Range header
                    try {

                        if (line.toLowerCase().startsWith("range: ")) {
                            isRangeFlag = true;
                            String byteString = line.split("=")[1]; // should isolate the xx-yy portion
                            String[] bytes = byteString.split("-");

                            // Handle unbounded case
                            if (bytes.length == 1) {
                                if (byteString.charAt(0) == '-') { // Ending block
                                    end = Integer.parseInt(bytes[0]);
                                } else { // Otherwise it is a startin g block
                                    start = Integer.parseInt(bytes[0]);
                                }
                            } else {
                                start = Integer.parseInt(bytes[0]);
                                end = Integer.parseInt(bytes[1]);
                            }

                            end++; // End is inclusive of byte count
                        }
                    } catch (NumberFormatException e) {
                        logger.error("Range header encountered non-integer byte range");
                        logger.error(e.getMessage());
                        sendErrorResponse(out, 400, "Bad Request", isHEAD);
                    }
                }

                // Can go back to inputStream because the reader so far has only finished reading the header
                // while loop on the length of content to read the remaining bytes from the message
                ByteArrayOutputStream byteContent = new ByteArrayOutputStream();
                // logger.debug("Attempting to read body bytes");
                // Read content-length # of content bytes
                for (int i = 0; i < contentLength; i++) {
                    inputByte = inputStream.read();

                    // If the # of bytes is incorrect
                    if (inputByte == -1) {
                        sendErrorResponse(out, 400, "Bad Request", isHEAD);
                        break; // Need to leave the loop if we reach end of request
                    }
                    byteContent.write(inputByte);
                }

                String messageBody = byteContent.toString();
                if (!messageBody.isEmpty()) {
                    logger.debug("Message body content: " + messageBody);
                }

                // Check for 400 error
                if (checkForErrors(messageContent, out, isHEAD, false)) {
                    logger.error("Error found in headers");
                    continue;
                }

                // Attempt to match method to route, otherwise send static
                String method = messageContent.get(0).split(" ")[0] + " " + messageContent.get(0).split(" ")[1];
                logger.debug("Method: " + method);
                Route methodRoute = matchRoute(method);

                if (methodRoute != null) {
                    logger.debug("Found associated route");

                    ResponseImpl resp = new ResponseImpl();
                    resp.setSocket(socket);
                    resp.setPrintWriter(out);

                    logger.debug("Message content: " + String.valueOf(messageContent));

                    // Make the headers map
                    Map<String, String> headersMap = new HashMap<>();
                    for (int j = 1; j < messageContent.size(); j++) {
                        String i = messageContent.get(j);
                        if (Objects.equals(i, "")) {
                            continue;
                        }
                        headersMap.put(i.split(": ")[0].toLowerCase(), i.split(": ")[1]);
                        headersMap.put(i.split(": ")[0].toLowerCase(), i.split(": ")[1]);
                        resp.header(i.split(": ")[0], i.split(": ")[1]);
                    }

                    // Update querymap with any params in the body
                    checkBody(contentType, messageBody);

                    // Create request/response objects
                    RequestImpl req = new RequestImpl(
                            messageContent.get(0).split(" ")[0],
                            messageContent.get(0).split(" ")[1],
                            messageContent.get(0).split(" ")[2],
                            headersMap,
                            queryMap,
                            paramsMap,
                            (InetSocketAddress) socket.getRemoteSocketAddress(),
                            byteContent.toByteArray(),
                            server
                    );

                    resp.setReq(req);

                    // If the session exists in server, add it to the request object
                    if (sessionCookie != null) {
                        req.setSession(sessionCookie);
                    }

                    try {
                        execBeforeFilters(req, resp);

                        Object object = null;
                        if (!resp.getIsWritten()) {
                            object = methodRoute.handle(req, resp);
                        }

                        execAfterFilters(req, resp);
                        // Check if I have already written
                        if (!resp.getIsWritten()) {
                            logger.debug("Sending output from thread");
                            // Send headers first
                            String outputHeader = "HTTP/1.1 " + resp.getStatusCode() + " " + resp.getStatusMessage()
                                    + "\r\n" + resp.getContentType() + "\r\n" + "Server: Andrew's Server\r\n";
                            //ADD CORS ACCESS
                            resp.header("Access-Control-Allow-Origin", "*");
                            resp.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
                            resp.header("Access-Control-Allow-Headers", "Content-Type");
                            for (String i : resp.getHeaders()) {
                                outputHeader += i + "\r\n";
                            }

//                            logger.debug("Headers added");

                            // If the cookie exists, send it back with the header
                            if (req.getCreated()) {
                                logger.debug("Sending session cookie: " + req.getSession().id());
                                outputHeader += "Set-Cookie: SessionID=" + req.getSession().id() + "\r\n";
                            }

                            byte[] bodyBytes = resp.getBody();
                            // Send back the body (b)
                            if (object != null) {
                                byte[] objectBytes = object.toString().getBytes();
                                outputHeader += "Content-Length:" + objectBytes.length + "\r\n\r\n";
                                out.print(outputHeader);
                                out.flush();
                                socket.getOutputStream().write(object.toString().getBytes());
                            } else if (bodyBytes.length != 0) {
                                // Send stored body value
                                outputHeader += "Content-Length:" + bodyBytes.length + "\r\n\r\n";
                                out.print(outputHeader);
                                out.flush();
                                socket.getOutputStream().write(bodyBytes);
                            }
//                            logger.debug("body sent");

                        } else {
                            logger.debug("Route handler already sent output, exiting");
                            break;
                        }
                    } catch (Exception e) {
                        // We only send exception if write() has not been called
                        if (!resp.getIsWritten()) {
                            sendErrorResponse(out, 500, "Internal Server Error", isHEAD);
                        }
                        logger.error("Error found in Route handler");
                        logger.error(e.getMessage());
                    }
                } else { // Static file
                    logger.debug("Static file serving (no route matched)");
                    // Checking for errors
                    if (checkForErrors(messageContent, out, isHEAD, true)) {
                        logger.error("Error found in headers (static check)");
                    } else {

                        // Read requested file
                        String path = messageContent.get(0).split(" ")[1];
                        File file = new File(rootPath + path);
                        String ext = path.split("\\.")[path.split("\\.").length - 1];

                        if (isIfModSinceFlag) {
                            Date lastMod = new Date(file.lastModified());
                            if (headerDate.before(lastMod)) {
                                sendFileResponse(out, file, socket, ext, 200, "OK", isHEAD, start, end);
                            } else {
                                sendErrorResponse(out, 304, "Not Modified", isHEAD);
                            }
                        } else if (isRangeFlag) {
                            sendFileResponse(out, file, socket, ext, 206, "Partial Content", isHEAD, start, end);
                        } else { //normal output
                            // Send the file back to client
                            sendFileResponse(out, file, socket, ext, 200, "OK", isHEAD, start, end);
                        }
                    }
                }
            }

            // Once we exit the while true from reading in -1, we will close the file and socket
            logger.debug("Socket has been closed");
            out.close();
            socket.close();
        } catch (IOException e) {
            logger.error("I/O Exception while handling client connection");
            logger.error(e.getMessage());
            logger.error(Arrays.toString(e.getStackTrace()));
        } catch (ParseException e) {
            logger.error("Parsing exception while checking if-modified-since header");
            logger.error(e.getMessage());
            logger.error(Arrays.toString(e.getStackTrace()));
        }
    }


    /** Given a method from client, see if it matches any routes
     * Includes parametric matching and query parameters
     * @param method2 String representing method from client
     * @return Route object from HashMap maintained by server
     */
    private Route matchRoute(String method2) {
        // Base case with no parametric tuning needed
        if (routes.containsKey(method2)) {
            return routes.get(method2);
        }

        // Otherwise start looking for parametric tuning
        // We know the params, if they exist, are at the back of the query
        String[] urlSplit = method2.split("\\?", 2);
        logger.debug("URL split method match: " + Arrays.toString(urlSplit));
        String method = urlSplit[0];

        if (urlSplit.length == 2) {
            String urlParams = urlSplit[1];
            // Parse out the urlParams
            String[] urlParamArr = urlParams.split("&");
            for (String param : urlParamArr) {
                String[] paramSplit = param.split("=", 2);
                if (paramSplit.length < 2) paramSplit = new String[] {paramSplit[0], ""};
                queryMap.put(decode(paramSplit[0], StandardCharsets.UTF_8), decode(paramSplit[1], StandardCharsets.UTF_8));
            }
        }

        String[] methodSplit = method.split("/");
        for (String route : routes.keySet()) {
            String[] routeSplit = route.split("/");
            // logger.debug("Route split: " + Arrays.toString(routeSplit));
            // If same number of "params"
            if (methodSplit.length == routeSplit.length) {
                boolean paramFlag = true;
                // Check that each param is the same
                for (int i = 0; i < methodSplit.length; i++) {
                    // They are either equal or the route is parametric (identified with :)
                    if (methodSplit[i].equals(routeSplit[i])) {
                        continue;
                    } else if (routeSplit[i].contains(":")) { // If a parameter, want to add it to the map
                        paramsMap.put(routeSplit[i].substring(1), methodSplit[i]);
                    } else {
                        paramFlag = false;
                        break;
                    }
                }
                if (paramFlag) {
                    return routes.get(route);
                }
            }
        }

        return null;
    }

    /**
     * First checks that the header is the right kind for reading parameters from body
     * If so, reads in query parameters from the body and adds them to the map
     *
     * @param contentType content type header from client
     * @param messageBody message body read in from client (could be empty)
     */
    private void checkBody(String contentType, String messageBody) {
        logger.debug("Checking body currently: " + contentType);
        // Add query params to map from body if content type matches
        if (contentType.equals("application/x-www-form-urlencoded")) {
            logger.debug("Found parameters in body, adding to query map");
            String[] bodyParams = messageBody.split("&");
            for (String param : bodyParams) {
                String[] paramSplit = param.split("=");
                queryMap.put(decode(paramSplit[0], StandardCharsets.UTF_8), decode(paramSplit[1], StandardCharsets.UTF_8));
            }
        }
    }

    private void execBeforeFilters(Request req, Response resp) {
        try {
            for (Filter f : server.getBeforeFilters()) {
                f.handle(req, resp);
            }
        } catch (Exception e) {
            logger.error("Error while executing before filters");
            logger.error(e.getMessage());
        }
    }

    private void execAfterFilters(Request req, Response resp) {
        try {
            for (Filter f : server.getAfterFilters()) {
                f.handle(req, resp);
            }
        } catch (Exception e) {
            logger.error("Error while executing after filters");
            logger.error(e.getMessage());
        }
    }


    private void sendFileResponse(PrintWriter out, File file, Socket socket, String ext,
                                  int statusCode, String statusMessage, Boolean isHEAD, int startByte, int endByte) throws IOException {
        logger.debug("Attempting to read: " + file.getAbsolutePath());
        if (!file.exists()) {
            sendErrorResponse(out, 404, "Not Found", isHEAD);
        } else if (!file.canRead()) {
            sendErrorResponse(out, 403, "Forbidden", isHEAD);
        } else if (isRangeFlag && (file.length() < endByte || startByte < 0 || startByte > endByte)) {
            sendErrorResponse(out, 416, "Range Not Satisfiable", isHEAD);
        } else {

            ByteArrayOutputStream fileContent = readFile(file, startByte, endByte);

            String type = switch (ext) {
                case "jpg", "jpeg" -> "image/jpeg";
                case "html" -> "text/html";
                case "txt" -> "text/plain";
                default -> "application/octet-stream";
            };

            // Send the header first through the PrintWriter
            long length = file.length();
            String outputHeader = "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +
                    "Content-Type: " + type + "\r\n" +
                    "Server: Andrew's Server\r\n" +
                    "Content-Length:" + length + "\r\n"
                    + "Access-Control-Allow-Origin: *\r\n" //Allow CORS
                    + "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                    + "Access-Control-Allow-Headers: Content-Type\r\n\r\n";

            if (isRangeFlag) {
                length = endByte - startByte;
                outputHeader = "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +
                        "Content-Type: " + type + "\r\n" +
                        "Server: Andrew's Server\r\n" +
                        "Content-Length:" + length + "\r\n" +
                        "Content-Range: bytes" + startByte + "-" + endByte + "/" + file.length() + "\r\n"
                        + "Access-Control-Allow-Origin: *\r\n" //Allow CORS
                        + "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                        + "Access-Control-Allow-Headers: Content-Type\r\n\r\n";
            }

            out.print(outputHeader);
            out.flush();

            // OutputStream for the file instead of having it go through the PrintWriter
            if (!isHEAD) {
                // send the file back through
                socket.getOutputStream().write(fileContent.toByteArray());
            }
        }
    }

    /**
     * Reads a range from a file (inclusive to the end) and returns file contents
     *
     * @param file  some file to read
     * @param start starting byte to read from
     * @param end   ending byte to read to (inclusive)
     * @return ByteArrayOutputStream containing the contents of the file
     */
    private ByteArrayOutputStream readFile(File file, int start, int end) {
        ByteArrayOutputStream fileContent = new ByteArrayOutputStream();
        try {
            BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(file));
            int byteRead;

            if (end == Integer.MAX_VALUE) { //end was not changed
                while ((byteRead = fileInput.read()) != -1) {
                    fileContent.write(byteRead);
                }
            } else { //end was changed
                // First we read up to the start in bytes to ignore
                for (int i = 0; i < start; i++) {
                    byteRead = fileInput.read();
                }
                for (int i = start; i < end; i++) {
                    byteRead = fileInput.read();
                    fileContent.write(byteRead);
                }
            }
        } catch (IOException e) {
            logger.error("I/O Exception while reading file to send back");
            logger.error(e.getMessage());
        }
        return fileContent;
    }

    /**
     * Helper function checks for errors given some message
     *
     * @param message  Headers from client to be parsed (pre-parsed into an array by line)
     * @param out      PrintWriter that output should be sent to (if there is an error)
     * @param isHEAD   Boolean for if the method has "HEAD" in it
     * @param isStatic We check for a few additional errors on a static request which we don't look for in a dynamic
     *                 request
     * @return Boolean, true if there is an error, false otherwise
     */
    private Boolean checkForErrors(List<String> message, PrintWriter out, Boolean isHEAD, Boolean isStatic) {
        logger.debug("Checking errors");
        if (message.isEmpty()) {
            sendErrorResponse(out, 400, "Bad Request", isHEAD);
            return true;
        }

        String[] requestLine = message.get(0).split(" ");
        if (requestLine.length != 3) {
            sendErrorResponse(out, 400, "Bad Request", isHEAD);
            return true;
        }

        String method = requestLine[0];
        String path = requestLine[1];
        String protocol = requestLine[2];
        String[] allowedMethods = {"GET", "HEAD", "POST", "PUT"};

        if (isStatic) {
            if (rootPath == null) {
                sendErrorResponse(out, 404, "Not Found", isHEAD);
                return true;
            }

            //403 Error
            if (path.contains("..")) {
                sendErrorResponse(out, 403, "Forbidden", isHEAD);
                return true;
            }

            //405 Error
            if (method.equals("POST") || method.equals("PUT")) {
                sendErrorResponse(out, 405, "Not Allowed", isHEAD);
                return true;
            }

            //501 Error
            if (!Arrays.asList(allowedMethods).contains(method)) {
                sendErrorResponse(out, 501, "Not Implemented", isHEAD);
                return true;
            }
        }

        //505 Error
        if (!protocol.equals("HTTP/1.1")) {
            sendErrorResponse(out, 505, "HTTP Version Not Supported", isHEAD);
            return true;
        }

        return false;
    }

    /**
     * Send an error to the client
     *
     * @param out          PrintWriter to send the error message to
     * @param statusCode   Integer status code for the error to use
     * @param errorMessage Error message to be placed in the error message box and header
     */
    private void sendErrorResponse(PrintWriter out, int statusCode, String errorMessage, Boolean isHEAD) {
        logger.debug("Inside sendErrorResponse function");
        if (isHEAD) {
            out.print("HTTP/1.1 " + statusCode + " " + errorMessage + "\r\n" +
                    "Content-Type: text/plain\r\n" +
                    "Server: Andrew's Server\r\n" +
                    "Content-Length: " + errorMessage.length() + "\r\n\r\n");
        } else {
            out.print("HTTP/1.1 " + statusCode + " " + errorMessage + "\r\n" +
                    "Content-Type: text/plain\r\n" +
                    "Server: Andrew's Server\r\n" +
                    "Content-Length: " + errorMessage.length() + "\r\n\r\n" +
                    errorMessage);
        }
        out.flush();
    }
}
