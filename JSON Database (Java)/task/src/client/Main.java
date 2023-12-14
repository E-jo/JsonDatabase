package client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    @Parameter(names = "-t", description = "Request type")
    String requestType;

    @Parameter(names = "-k", description = "Request key")
    String requestKey;

    @Parameter(names = "-v", description = "Request value")
    String requestValue;

    @Parameter(names = "-in", description = "File read")
    String fileName;

    static final String address = "127.0.0.1";
    static final int port = 22222;
    static final String filePath = "./src/client/data/";


    public static void main(String[] args) throws IOException, InterruptedException {
        Main main = new Main();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.send();
    }

    public void send() throws IOException {
        JsonObject request = new JsonObject();

        try {
            switch (requestType) {
                case "set" -> {
                    request.addProperty("type", requestType);
                    request.addProperty("key", requestKey);
                    request.addProperty("value", requestValue);
                }
                case "delete", "get" -> {
                    request.addProperty("type", requestType);
                    request.addProperty("key", requestKey);
                }
                case "exit" -> request.addProperty("type", requestType);
            }
        } catch (Exception e) {
            if (fileName.isBlank()) {
                return;
            } else {
                Path inputFilePath = Paths.get(filePath + fileName);
                File inputFile = inputFilePath.toFile();
                BufferedReader in = new BufferedReader(new FileReader(inputFile));
                String fileInputStr = in.readLine();
                in.close();
                Gson gson = new Gson();
                request = gson.fromJson(fileInputStr, JsonObject.class);
            }
        }

        Socket socket = new Socket(InetAddress.getByName(address), port);
        DataInputStream input = new DataInputStream(socket.getInputStream());
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        System.out.println("Client started!");

        Gson gson = new Gson();
        String recordRequest = gson.toJson(request);

        output.writeUTF(recordRequest);
        System.out.println("Sent: " + recordRequest);
        String response = input.readUTF();
        System.out.println("Received: " + response);
        socket.close();
    }

}
