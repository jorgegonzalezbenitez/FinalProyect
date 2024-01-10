package org.example.Model;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

public class Command implements CommandSet {

    private static  String path;

    public Command(String path) {
        this.path = path;
    }

    @Override
    public  void WeatherDataQuery(String location, String checkIn, String checkOut) {
        try (Connection connection = DriverManager.getConnection(path)) {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT AVG(cloud) AS cloud, AVG(windSpeed) AS windspeed, AVG(temp) AS temp, AVG(humidity) AS humidity, AVG(propRain) AS propRain " +
                            "FROM Weather " +
                            "WHERE name = ? AND instant >= ? AND instant <= ?");

            statement.setString(1, location);
            statement.setString(2, checkIn);
            statement.setString(3, checkOut);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String avgCloud = resultSet.getString("cloud");
                    String avgWindSpeed = resultSet.getString("windspeed");
                    String avgTemp = resultSet.getString("temp");
                    String avgHumidity = resultSet.getString("humidity");
                    String avgPropRain = resultSet.getString("propRain");

                    System.out.println("Tiempo medio de los datos para " + location +
                            " durante el periodo de reserva (" + checkIn +
                            " hasta " + checkOut + "):");
                    System.out.println("Promedio de nubes: " + avgCloud);
                    System.out.println("Promedio de velocidad del viento: " + avgWindSpeed);
                    System.out.println("Promedio de temperatura: " + avgTemp);
                    System.out.println("Promedio de humedad: " + avgHumidity);
                    System.out.println("Promedio de probabilidad de precipitaciones: " + avgPropRain);
                    System.out.println("----------------------------------------");
                } else {
                    System.out.println("Datos del tiempo no disponible");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void HotelDataQuery(String country, String checkIn, String checkOut) {
        try {
            LocalDate checkInDate = LocalDate.parse(checkIn);
            LocalDate checkOutDate = LocalDate.parse(checkOut);

            int dias = (int) checkInDate.until(checkOutDate).getDays();

            try (Connection connection = DriverManager.getConnection(path);
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT DISTINCT rate, tax, hotelName, rateName "
                                 + "FROM Hotel "
                                 + "WHERE country = ? AND checkIn = ?")) {

                statement.setString(1, country);
                statement.setString(2, checkIn);

                ResultSet resultSet = statement.executeQuery();

                Map<String, HotelInfo> mejorHotelPorHotel = new HashMap<>();

                while (resultSet.next()) {
                    int rate = resultSet.getInt("rate");
                    int tax = resultSet.getInt("tax");
                    String hotelName = resultSet.getString("hotelName");
                    String rateName = resultSet.getString("rateName");

                    int precioTotal = (rate * dias) + tax;

                    if (!mejorHotelPorHotel.containsKey(hotelName) || precioTotal < mejorHotelPorHotel.get(hotelName).getPrecioTotal()) {
                        HotelInfo hotelInfo = new HotelInfo(precioTotal, hotelName, rateName,rate);
                        mejorHotelPorHotel.put(hotelName, hotelInfo);
                    }
                }

                for (HotelInfo hotelInfo : mejorHotelPorHotel.values()) {
                    System.out.println("Hotel recomendado en " + country + ":");
                    System.out.println("Precio total a pagar: " + hotelInfo.getPrecioTotal());
                    System.out.println("Precio por noche: " + hotelInfo.getRate());
                    System.out.println("Fecha de entrada: " + checkIn);
                    System.out.println("Fecha de salida: " + checkOut);
                    System.out.println("Número de días de estancia: " + dias);
                    System.out.println("Nombre del hotel: " + hotelInfo.getHotelName());
                    System.out.println("Nombre de la página Web: " + hotelInfo.getRateName());
                    System.out.println("----------------------------------------");
                }

                if (mejorHotelPorHotel.isEmpty()) {
                    System.out.println("No hay hoteles disponibles para la fecha especificada.");
                }

            }

        } catch (SQLException | DateTimeParseException e) {
            e.printStackTrace();
        }
    }
}
