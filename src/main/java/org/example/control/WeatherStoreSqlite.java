package org.example.control;
import org.example.model.*;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
class WeatherStoreSqlite implements WeatherStore {



    @Override
    public void save(Location location, Instant instant) {
        WeatherProvider weatherProvider = new WeatherMapProvider(WeatherMapProvider.getAPI_KEY());
        Weather weather = weatherProvider.getWeather(location, instant);

        if (weather != null) {
            try {
                Connection connection = DriverManager.getConnection("jdbc:sqlite:C:/Users/cgsos/OneDrive/Documentos/db_weather/db_weatherr.db");

                String tableName =  location.getName().toLowerCase().replace(" ", "_");
                String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                        "temperature REAL," +
                        "humidity INTEGER," +
                        "clouds INTEGER," +
                        "wind REAL," +
                        "pop REAL," +
                        "instant TEXT" +
                        ")";

                System.out.println("Created table for " + location.getName());
                Statement statement = connection.createStatement();
                statement.executeUpdate(createTableSQL);
                if (WeatherDataExist(connection, tableName, instant.toString())) {
                    String updateWeatherSQL = "UPDATE " + tableName + " SET temperature=?, humidity=?, clouds=?, wind=?, pop=? " +
                            "WHERE instant=?";
                    PreparedStatement updateStatement = connection.prepareStatement(updateWeatherSQL);

                    updateStatement.setDouble(1, weather.getTemp());
                    updateStatement.setInt(2, weather.getHumidity());
                    updateStatement.setInt(3, weather.getCloud());
                    updateStatement.setDouble(4, weather.getSpeed());
                    updateStatement.setDouble(5, weather.getPop());
                    updateStatement.setString(6, instant.toString());

                    updateStatement.executeUpdate();
                } else {
                    String insertWeatherSQL = "INSERT INTO " + tableName + " (temperature, humidity, clouds, wind, pop, instant)" +
                            " VALUES (?, ?, ?, ?, ?, ?)";
                    PreparedStatement insertStatement = connection.prepareStatement(insertWeatherSQL);

                    insertStatement.setDouble(1, weather.getTemp());
                    insertStatement.setInt(2, weather.getHumidity());
                    insertStatement.setInt(3, weather.getCloud());
                    insertStatement.setDouble(4, weather.getSpeed());
                    insertStatement.setDouble(5, weather.getPop());
                    insertStatement.setString(6, instant.toString());

                    insertStatement.executeUpdate();
                }

                connection.close();

            } catch (SQLException | NullPointerException exc) {
                exc.printStackTrace();
            }
        } else {
            System.out.println("No weather data found for " + location.getName() + " at " + instant);
        }
    }

    private static boolean WeatherDataExist(Connection connection, String tableName, String instant)
            throws SQLException {
        String checkSQL = "SELECT COUNT(*) FROM " + tableName + " WHERE instant = ?";
        PreparedStatement checkStatement = connection.prepareStatement(checkSQL);
        checkStatement.setString(1, instant);
        ResultSet resultSet = checkStatement.executeQuery();

        return resultSet.getInt(1) > 0;
    }
}