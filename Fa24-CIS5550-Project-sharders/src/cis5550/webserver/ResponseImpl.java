package cis5550.webserver;

import cis5550.tools.Logger;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class ResponseImpl implements Response {

    private ArrayList<String> headers = new ArrayList<String>();
    private int statusCode = 200;
    private String statusMessage = "OK";
    private String contentType = "Content-Type: text/html";
    private static final Logger logger = Logger.getLogger(ResponseImpl.class);

    private Boolean isWritten = false;
    private byte[] bodyBytes = null;
    private Socket socket;
    private PrintWriter out;

    private RequestImpl req;

    /**
     * Set a string body, keeps latest call
     *
     * @param body string, to set var
     */
    @Override
    public void body(String body) {
        if (!isWritten) {
            bodyBytes = body.getBytes();
        }
    }


    /**
     * Set a byte array, keeps latest call
     *
     * @param bodyArg byte array, to set var
     */
    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (!isWritten) {
            bodyBytes = bodyArg;
        }
    }

    /**
     * Adds a line "name: value" to the headers array for return later
     *
     * @param name  first part of the header
     * @param value second part of the header
     */
    @Override
    public void header(String name, String value) {
        if (!isWritten) {
            headers.add(name + ": " + value);
        }
    }

    /**
     * Keep the lastest call for a content type return
     *
     * @param contentType content type for message return
     */
    @Override
    public void type(String contentType) {
        if (!isWritten) {
            this.contentType = "Content-Type: " + contentType;
        }
    }

    /**
     * Keeps latest call of status internally
     *
     * @param statusCode   integer status
     * @param reasonPhrase corresponding reason phrase
     */
    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!isWritten) {
            this.statusCode = statusCode;
            this.statusMessage = reasonPhrase;
        }
    }

    public void setReq(RequestImpl r) {
        req = r;
    }

    /** Writes an output to socket, with headers if this is the first time (after first time you cannot change
     * anything in the response object)
     * @param b byte array to write to socket
     */
    @Override
    public void write(byte[] b) throws Exception {
        // if not yet written, send back headers
        if (!isWritten) {
            // Send back the headers
            logger.debug("Writing header from handler");
            isWritten = true;
            String outputHeader = "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +
                    contentType + "\r\n" +
                    "Server: Andrew's Server\r\n";

            // If the request created a header, we want to send that back
            if (req.getCreated()) {
                outputHeader += "Set-Cookie: SessionID=" + req.getSession().id() + "\r\n";
            }

            for (String i : headers) {
                outputHeader += i + "\r\n";
            }
            outputHeader += "Connection: close\r\n\r\n";

            out.print(outputHeader);
            out.flush();
        }
        // Send back byte array b to client
        // logger.debug("Called with: " + Arrays.toString(b));
        socket.getOutputStream().write(b);
    }

    public Boolean getIsWritten() {
        return isWritten;
    }

    public byte[] getBody() {
        return bodyBytes;
    }

    public ArrayList<String> getHeaders() {
        return headers;
    }

    public void setPrintWriter(PrintWriter socketWriter) {
        out = socketWriter;
    }

    public void setSocket(Socket socketIn) {
        socket = socketIn;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getContentType() {
        return contentType;
    }

    /** Send a message back to client redirecting them to another url
     * @param url url to redirect them to
     * @param responseCode integer response code which matches some existing
     */
    @Override
    public void redirect(String url, int responseCode) {
        String responseMessage = switch (responseCode) {
            case 301 -> "Moved Permanently";
            case 302 -> "Found";
            case 303 -> "See Other";
            case 307 -> "Temporary Redirect";
            case 308 -> "Permanent Redirect";
            default -> "Redirect";
        };

        if (!isWritten) {
            this.statusCode = responseCode;
            this.statusMessage = responseMessage;

            String outputHeader = "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +
                    "Location: " + url + "\r\n" +
                    contentType + "\r\n" +
                    "Content-Length: 0\r\n" +
                    "Server: Andrew's Server\r\n";

            for (String i : headers) {
                outputHeader += i + "\r\n";
            }

            outputHeader += "Connection: close\r\n\r\n";

            out.print(outputHeader);
            out.flush();
            isWritten = true;
        }
    }

    /** Should only be called by a before() lambda, which can be used to halt the route handler execution
     * @param statusCode integer, code to throw
     * @param reasonPhrase statusMessage to return with the statusCode
     */
    @Override
    public void halt(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.statusMessage = reasonPhrase;

        String outputHeader = "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +
                contentType + "\r\n" +
                "Content-Length: 0\r\n" +
                "Server: Andrew's Server\r\n";

        for (String i : headers) {
            outputHeader += i + "\r\n";
        }

        outputHeader += "Connection: close\r\n\r\n";

        out.print(outputHeader);
        out.flush();
        isWritten = true;
    }
}
