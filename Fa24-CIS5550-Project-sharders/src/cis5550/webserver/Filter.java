package cis5550.webserver;

public interface Filter {
    void handle(Request request, Response response) throws Exception;
}
