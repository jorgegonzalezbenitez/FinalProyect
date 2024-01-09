package org.example;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DataLake implements Listener{
    private  String directory;

    public DataLake(String directory) {
        this.directory = directory;
    }
    @Override
    public void consume(String message,String topicName) {
        System.out.println("Message received");
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
        String newTS = jsonObject.get("ts").getAsString();
        String newSS = jsonObject.get("ss").getAsString();

        Instant instant = Instant.parse(newTS);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDate = dateTime.format(formatter);

        String directoryPath = directory+ File.separator + "datalake" + File.separator + "eventstore" + File.separator + topicName + File.separator + newSS;
        createDirectory(directoryPath);

        String filePath = directoryPath + File.separator + formattedDate + ".events";
        writeMessageToFile(filePath, message);
    }

    private void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
            System.out.println("Directory created");
        }
    }

    private void writeMessageToFile(String filePath, String message) {
        try (FileWriter writer = new FileWriter(filePath, true)) {
            writer.write(message + "\n");
            System.out.println("Message written: " + filePath);
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
    }
}