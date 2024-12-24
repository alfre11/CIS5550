package cis5550.webserver;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import cis5550.tools.Logger;

public class ResponseImpl implements Response{

	private int statusCode = 200;
    private String reasonPhrase = "OK";
    private List<String> headers = new ArrayList<>();
    private byte[] bodyBytes = null;
    private boolean bodySet = false;
    private boolean writeCalled = false; 
    private static final Logger logger = Logger.getLogger(Server.class);
    private boolean headersWritten = false;
    private OutputStream outStream;
	
    public ResponseImpl(OutputStream outStream) {
        this.outStream = outStream;
    }
    
	@Override
	public void body(String body) {
		logger.info("response body called: " + body);
		if (!writeCalled) {
            this.bodyBytes = body.getBytes();
            logger.info("bodyBytes: " + new String (bodyBytes));
            this.bodySet = true;
            header("Content Length", String.valueOf(bodyBytes.length));
        }
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		logger.info("response bodyAsBytes called");
		if (!writeCalled) {
            this.bodyBytes = bodyArg;
            this.bodySet = true;
            header("Content Length", String.valueOf(bodyBytes.length));
        }
	}

	@Override
	public void header(String name, String value) {
		logger.info("response header called");
		if (!writeCalled) {
            headers.add(name + ": " + value);
        }
	}

	@Override
	public void type(String contentType) {
		logger.info("response type called");
		if(!writeCalled) {
			header("Content-Type", contentType);
		}
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		logger.info("response status called");
		if (!writeCalled) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
	}

	@Override
	public void write(byte[] b) throws Exception {
		logger.info("response write called");
		//bodyBytes += b;
		if (!headersWritten) {
            // Write status line
            outStream.write(("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n").getBytes());

            // Write headers
            for (String header : headers) {
                outStream.write((header + "\r\n").getBytes());
            }

            // End headers
            outStream.write("\r\n".getBytes());

            headersWritten = true;
            logger.info("headers written with write");
        }
  
        // Write the body
        outStream.write(b);
        outStream.flush();
        writeCalled = true;
		logger.info("write written");
	}

	@Override
	public void redirect(String url, int responseCode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		// TODO Auto-generated method stub
		
	}
	
	public String getBody() {
		return new String (bodyBytes);
	}
	
	public List<String> getHeader() {
		return headers;
	}
	
	public String getStatus() {
		return String.valueOf(statusCode);
	}
	
	public String getReason() {
		return reasonPhrase;
	}
	
	public boolean getWriteCalled() {
		return writeCalled;
	}
}
