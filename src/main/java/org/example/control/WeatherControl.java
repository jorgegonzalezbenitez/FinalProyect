package org.example.control;

import org.example.model.*;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class WeatherControl {
    public WeatherProvider weatherProvider;

    public WeatherControl(WeatherProvider weatherProvider) {
        this.weatherProvider = weatherProvider;
    }

    public void execute() {
        Location gran_canaria = new Location("Gran Canaria", 28.09973, -15.41343);
        Location tenerife = new Location("Tenerife", 28.46824, -16.25462);
        Location hierro = new Location("El hierro", 27.80628, -17.91578);
        Location gomera = new Location("La Gomera", 28.09163, -17.11331);
        Location fuerteventura = new Location("Fuerteventura", 28.50038, -13.86272);
        Location palma = new Location("La Palma", 28.68351, -17.76421);
        Location lanzarote = new Location("Lanzarote", 28.96302, -13.54769);
        Location graciosa = new Location("La Graciosa", 29.23147, -13.50341);

        List<Location> canary_islands = List.of(gran_canaria, tenerife,lanzarote,gomera,fuerteventura,palma,hierro,graciosa);

        
        ArrayList<Weather> weathers =new ArrayList<>();
        ArrayList<Instant> instants =new ArrayList<>();
        
        getWeatherCall(instants,canary_islands,weathers);
        createInstant(instants);
        loadCall(instants,canary_islands);
    }

    public static void loadCall(ArrayList<Instant> instants, List<Location> canary_islands) {
        WeatherStore weatherStore = new WeatherStoreSqlite();
        for (Location iteredLocation : canary_islands) {
            for (Instant iteredInstant : instants) {
                weatherStore.save(iteredLocation, iteredInstant);
            }
        }
    }

    public static ArrayList<Instant> createInstant(ArrayList<Instant> instants) {
        for (int i = 0; i < 5; i++) {
            LocalDate hoy = LocalDate.now();
            LocalTime hour = LocalTime.of(12, 0);
            LocalDateTime todayHour = LocalDateTime.of(hoy, hour);
            Instant todayInstant = todayHour.toInstant(ZoneOffset.UTC);
            Instant tomorrowInstant = todayInstant.plus(i, ChronoUnit.DAYS);
            instants.add(tomorrowInstant);
        }
        return instants;
    }

    public static ArrayList<Weather> getWeatherCall(ArrayList<Instant> instants, List<Location> canary_islands, ArrayList<Weather> weathers) {

        WeatherProvider weatherProvider = new WeatherMapProvider();
        for (Location iteredLocation : canary_islands) {
            for (Instant iteredInstant : instants) {
                Weather weather = weatherProvider.getWeather(iteredLocation, iteredInstant);

                if (weather != null) {
                    System.out.println("Weather for " + iteredLocation.getName() + " at " + iteredInstant + ":");
                    System.out.println(weather);
                    System.out.println("\n");
                } else {
                    System.out.println("No weather data found for " + iteredLocation.getName() + " at " + iteredInstant);
                }
                weathers.add(weather);
            }
        }
        return weathers;
    }
    public static void main(String[] args) {
        WeatherControl weatherControl = new WeatherControl(new WeatherMapProvider());
        weatherControl.execute();
    }
}
