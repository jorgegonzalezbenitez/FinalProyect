package org.example.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Datamart implements DatabaseStore {
    private final String path;
    public Datamart(String path) {
        this.path = path;
    }

    @Override
    public void weatherStore(String message) {
        try {
            JsonObject jsonObject = new Gson().fromJson(message, JsonObject.class);
            JsonObject location = jsonObject.getAsJsonObject("location");
            String name = location.getAsJsonPrimitive("name").getAsString();
            String timestamp = jsonObject.getAsJsonPrimitive("ts").getAsString();
            String ss = jsonObject.getAsJsonPrimitive("ss").getAsString();
            String lat = location.getAsJsonPrimitive("lat").getAsString();
            String lon = location.getAsJsonPrimitive("lon").getAsString();
            String temp = jsonObject.getAsJsonPrimitive("temp").getAsString();
            String humidity = jsonObject.getAsJsonPrimitive("humidity").getAsString();
            String cloud = jsonObject.getAsJsonPrimitive("cloud").getAsString();
            String windSpeed = jsonObject.getAsJsonPrimitive("windSpeed").getAsString();
            String propRain = jsonObject.getAsJsonPrimitive("propRain").getAsString();
            String instant = jsonObject.getAsJsonPrimitive("instant").getAsString();



            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.parse(timestamp), ZoneId.systemDefault());
            String ts = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd/HH"));

            try (Connection connection = DriverManager.getConnection(path)) {
                String tableName = "Weather";
                try {
                    String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                            "name TEXT," +
                            "cloud TEXT," +
                            "windSpeed TEXT," +
                            "temp TEXT," +
                            "humidity TEXT," +
                            "instant TEXT," +
                            "propRain TEXT," +
                            "ts TEXT," +
                            "ss TEXT," +
                            "lat TEXT," +
                            "lon TEXT" +
                            ")";

                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(createTableSQL);
                        System.out.println("Table create for: " + tableName);
                    }
                } catch (SQLException exc) {
                    exc.printStackTrace();
                }

                connection.setAutoCommit(false);

                try {
                    String deleteRowsSQL = "DELETE FROM " + tableName + " WHERE ts <> ?";
                    try (PreparedStatement deleteRowsStatement = connection.prepareStatement(deleteRowsSQL)) {
                        deleteRowsStatement.setString(1, ts);
                        deleteRowsStatement.executeUpdate();
                    }

                    String insertWeatherSQL = "INSERT INTO " + tableName + " (name, cloud, windSpeed, temp, humidity, instant, propRain, ts, ss, lat, lon) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement insertStatement = connection.prepareStatement(insertWeatherSQL)) {
                        insertStatement.setString(1, name);
                        insertStatement.setString(2, cloud);
                        insertStatement.setString(3, windSpeed);
                        insertStatement.setString(4, temp);
                        insertStatement.setString(5, humidity);
                        insertStatement.setString(6, instant);
                        insertStatement.setString(7, propRain);
                        insertStatement.setString(8, ts);
                        insertStatement.setString(9, ss);
                        insertStatement.setString(10, lat);
                        insertStatement.setString(11, lon);

                        insertStatement.executeUpdate();
                    }

                    connection.commit();
                    System.out.println("Database datamart.db created");
                } catch (SQLException exc) {
                    connection.rollback();
                    exc.printStackTrace();
                } finally {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException | NullPointerException exc) {
                exc.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    @Override
    public void hotelStore(String message) {
        try {
            JsonObject jsonObject = new Gson().fromJson(message, JsonObject.class);
            JsonArray ratesArray = jsonObject.getAsJsonArray("rates");
            JsonObject dateObject = jsonObject.getAsJsonObject("date");
            String country = dateObject.getAsJsonPrimitive("country").getAsString();
            String hotelName = dateObject.getAsJsonPrimitive("name").getAsString();
            String hotelkey = dateObject.getAsJsonPrimitive("hotelKey").getAsString();
            String checkIn = dateObject.getAsJsonPrimitive("checkIn").getAsString();
            String checkOut = dateObject.getAsJsonPrimitive("checkOut").getAsString();

            String timestamp = jsonObject.getAsJsonPrimitive("ts").getAsString();
            String ss = jsonObject.getAsJsonPrimitive("ss").getAsString();

            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.parse(timestamp), ZoneId.systemDefault());
            String ts = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd/HH"));

            try (Connection connection = DriverManager.getConnection(path)) {
                try {
                    String createTableSQL = "CREATE TABLE IF NOT EXISTS Hotel(" +
                            "country TEXT," +
                            "hotelName TEXT," +
                            "hotelKey TEXT," +
                            "checkIn TEXT," +
                            "checkOut TEXT," +
                            "ss TEXT," +
                            "ts TEXT," +
                            "code TEXT," +
                            "rateName TEXT," +
                            "rate INTEGER," +
                            "tax INTEGER" +
                            ")";

                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(createTableSQL);
                        System.out.println("Hotel table created");
                    }
                } catch (SQLException exc) {
                    exc.printStackTrace();
                }

                connection.setAutoCommit(false);

                try {
                    String deleteRowsSQL = "DELETE FROM Hotel WHERE ts <> ?";
                    try (PreparedStatement deleteRowsStatement = connection.prepareStatement(deleteRowsSQL)) {
                        deleteRowsStatement.setString(1, ts);
                        deleteRowsStatement.executeUpdate();
                    }

                    for (JsonElement rateElement : ratesArray) {
                        JsonObject rateObject = rateElement.getAsJsonObject();
                        String code = rateObject.getAsJsonPrimitive("code").getAsString();
                        String rateName = rateObject.getAsJsonPrimitive("rateName").getAsString();
                        String rate = rateObject.getAsJsonPrimitive("rate").getAsString();
                        String tax = rateObject.getAsJsonPrimitive("tax").getAsString();

                        String insertRateSQL = "INSERT INTO Hotel (country, hotelName, hotelKey, checkIn, checkOut, ss, ts, code, rateName, rate, tax) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement insertStatement = connection.prepareStatement(insertRateSQL)) {
                            insertStatement.setString(1, country);
                            insertStatement.setString(2, hotelName);
                            insertStatement.setString(3, hotelkey);
                            insertStatement.setString(4, checkIn);
                            insertStatement.setString(5, checkOut);
                            insertStatement.setString(6, ss);
                            insertStatement.setString(7, ts);
                            insertStatement.setString(8, code);
                            insertStatement.setString(9, rateName);
                            insertStatement.setString(10, rate);
                            insertStatement.setString(11, tax);

                            insertStatement.executeUpdate();
                        }
                    }

                    connection.commit();
                } catch (SQLException exc) {
                    connection.rollback();
                    exc.printStackTrace();
                } finally {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException | NullPointerException exc) {
                exc.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}