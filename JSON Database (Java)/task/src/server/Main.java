package server;

import com.google.gson.*;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Main {
    static final String address = "127.0.0.1";
    static final int port = 22222;
    private static final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    static boolean closeServer = false;
    static JsonObject jsonDb = new JsonObject();
    static Gson gson = new Gson();

    static final String filePath = "C:\\Users\\erics\\IdeaProjects\\JSON Database (Java)\\JSON Database (Java)\\task\\src\\server\\db.json";
    //static final String filePath = "./JSON Database/task/src/server/db.json";
    //static final String filePath = "./src/server/db.json";

    public static class ClientHandler implements Runnable {
        BufferedReader reader;
        Socket socket;

        public ClientHandler(Socket clientSocket) {
            try {
                this.socket = clientSocket;
                InputStreamReader isReader = new InputStreamReader(socket.getInputStream());
                this.reader = new BufferedReader(isReader);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void run() {
            String recordRequest;
            JsonObject clientInput;
            try {
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream output  = new DataOutputStream(socket.getOutputStream());
                recordRequest = input.readUTF();
                clientInput = gson.fromJson(recordRequest, JsonObject.class);
                System.out.println("Received: " + clientInput.toString());
                String requestType = String.valueOf(clientInput.get("type"))
                        .toLowerCase()
                        .replaceAll("\"", "");
                System.out.println("Request type: " + requestType);

                switch (requestType) {
                    case "get" -> getFromDb(clientInput, output);
                    case "set" -> writeToDb(clientInput, output);
                    case "delete" -> deleteFromDb(clientInput, output);
                    case "exit" -> {
                        closeServer = true;
                        System.out.println("Exit received");
                        Map<String, String> response = new HashMap<>();
                        response.put("response", "OK");
                        response.put("value", "Exiting");
                        String serverResponse = gson.toJson(response);
                        output.writeUTF(serverResponse);
                    }
                    default -> System.out.println("No type match found");
                }
                socket.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Server started!");
        try {
            // first two lines here can be commented out to persist the db between server sessions
            jsonDb = new JsonObject();
            serialize(jsonDb.toString(), filePath);
            String dbString = (String) deserialize(filePath);
            jsonDb = gson.fromJson(dbString, JsonObject.class);
            System.out.println("DB loaded");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        ServerSocket server = new ServerSocket(port, 50, InetAddress.getByName(address));
        while (!closeServer) {
            Socket socket = server.accept();
            executorService.submit(new ClientHandler(socket));
            executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
            //System.out.println(closeServer);
        }
        server.close();
        System.out.println("Server closing");
        System.exit(0);
    }

    public static void getFromDb(JsonObject userInput,
                                     DataOutputStream output) throws IOException {
        rwl.readLock().lock();
        try {
            String dbString = (String) deserialize(filePath);
            jsonDb = gson.fromJson(dbString, JsonObject.class);
            System.out.println("DB loaded");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            rwl.readLock().unlock();
        }

        JsonElement targetKey = userInput.get("key");
        System.out.println("Target key: " + targetKey);

        JsonObject response = new JsonObject();
        JsonArray complexKeyPath;
        boolean isPrimitiveValue = false;
        String primitiveValue = "";

        if (targetKey.isJsonPrimitive()) {
            System.out.println("JsonPrimitive keyPath found");
            primitiveValue = targetKey.getAsString();
            isPrimitiveValue = true;
            if (!jsonDb.has(String.valueOf(targetKey))) {
                response.addProperty("response", "ERROR");
                response.addProperty("reason", "No such key");
            }
        }

        JsonElement targetValue = null;
        if (targetKey.isJsonArray()) {
            System.out.println("JsonArray keyPath found");
            complexKeyPath = targetKey.getAsJsonArray();
            targetValue = getNestedJsonValue(complexKeyPath);
        }
        if (targetValue == null) {
            response.addProperty("response", "ERROR");
            response.addProperty("reason", "No such key");
        } else {
            response.addProperty("response", "OK");
        }
        if (isPrimitiveValue) {
            response.addProperty("value", primitiveValue);
        } else if (targetValue != null){
            if (targetValue.isJsonObject()) {
                response.add("value", targetValue);
            } else {
                response.addProperty("value",
                        String.valueOf(targetValue).replaceAll("\"", ""));
            }
        }


        String serverResponse = gson.toJson(response);
        output.writeUTF(serverResponse);
        //System.out.println("Responding: " + serverResponse);
    }

    public static JsonElement getNestedJsonValue(JsonArray complexKeyPath) {
        JsonElement currentElement = jsonDb;

        for (JsonElement keyElement : complexKeyPath) {
            System.out.println("Key: " + keyElement);
            if (currentElement.isJsonObject()) {
                String key = keyElement.getAsString();
                if (((JsonObject) currentElement).has(key)) {
                    System.out.println("Key '" + key + "' found");
                } else {
                    System.out.println("Key '" + key + "' not found");
                }
                currentElement = ((JsonObject) currentElement).get(key);
                System.out.println("Current element: " + currentElement);
            } else {
                return null;
            }
        }

        return currentElement;
    }

    public static void traverseAndModify(JsonArray complexKeyPath,
                                         JsonElement newValue) {

        String[] keys = new String[complexKeyPath.size()];
        for (int i = 0; i < complexKeyPath.size(); i++) {
            keys[i] = complexKeyPath.get(i).getAsString().replaceAll("\"", "");
        }
        JsonObject currentObject = jsonDb;
        for (int i = 0; i < keys.length - 1; i++) {
            if (!currentObject.has(keys[i])) {
                currentObject.add(keys[i], new JsonObject());
            }
            currentObject = currentObject.getAsJsonObject(keys[i]);
        }
        currentObject.add(keys[keys.length - 1], newValue);
    }

    public static void writeToDb(JsonObject userInput,
                                 DataOutputStream output) throws IOException {

        rwl.readLock().lock();
        try {
            String dbString = (String) deserialize(filePath);
            jsonDb = gson.fromJson(dbString, JsonObject.class);
            System.out.println("DB loaded");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            rwl.readLock().unlock();
        }

        JsonObject response = new JsonObject();
        JsonElement targetKey = userInput.get("key");
        rwl.writeLock().lock();
        try {
            System.out.println("Writing to keyPath: " + targetKey);

            if (targetKey.isJsonPrimitive()) {
                jsonDb.add(targetKey.getAsString().replaceAll("\"", ""), userInput.get("value"));
                System.out.println("Adding top-level key: " + targetKey + " value: " + userInput.get("value"));
            }

            if (targetKey.isJsonArray()) {
                traverseAndModify(targetKey.getAsJsonArray(), userInput.get("value"));
            }

            serialize(jsonDb.toString(), filePath);

            System.out.println("DB written");
            Gson formattedGson = new GsonBuilder()
                    .setPrettyPrinting()
                    .create();

            System.out.println(formattedGson.toJson(jsonDb));

            response.addProperty("response", "OK");

        } catch (Exception e) {
            e.printStackTrace();
            response.addProperty("response", "ERROR");
        } finally {
            rwl.writeLock().unlock();
        }
        String serverResponse = gson.toJson(response);
        output.writeUTF(serverResponse);
        //System.out.println("Responding: " + serverResponse);
    }

    public static boolean deleteKey(JsonObject jsonObject, JsonArray targetKey) {
        JsonObject currentObject = jsonObject;
        int i = 0;
        for (; i < targetKey.size() - 1; i++) {
            if (currentObject.has(targetKey.get(i).getAsString())) {
                currentObject = currentObject.getAsJsonObject(targetKey.get(i).getAsString());
            } else {
                return false;
            }
        }
        if (currentObject.has(targetKey.get(i).getAsString())) {
            currentObject.remove(targetKey.get(i).getAsString());
            return true;
        } else {
            return false;
        }
    }

    public static void deleteFromDb(JsonObject userInput,
                                 DataOutputStream output) throws IOException {

        rwl.readLock().lock();
        try {
            String dbString = (String) deserialize(filePath);
            jsonDb = gson.fromJson(dbString, JsonObject.class);
            System.out.println("DB loaded");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            rwl.readLock().unlock();
        }

        JsonObject response = new JsonObject();
        rwl.writeLock().lock();
        try {
            if (deleteKey(jsonDb, userInput.getAsJsonArray("key"))) {
                response.addProperty("response", "OK");
            } else {
                response.addProperty("response", "ERROR");
                response.addProperty("reason", "No such key");
            }

            serialize(jsonDb.toString(), filePath);
        } finally {
            rwl.writeLock().unlock();
        }

        String serverResponse = gson.toJson(response);
        output.writeUTF(serverResponse);
        //System.out.println("Responding: " + serverResponse);
    }

    public static void serialize(Object obj, String fileName) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.close();
    }

    public static Object deserialize(String fileName) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(fileName);
        BufferedInputStream bis = new BufferedInputStream(fis);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }
}
