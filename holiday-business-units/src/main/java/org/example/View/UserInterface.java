package org.example.View;
import org.example.Model.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;

public class UserInterface {
    public  CommandSet commandSet;

    public UserInterface(CommandSet commandSet) {
        this.commandSet = commandSet;
    }

    public void execute( ){
        System.out.println("--------------------------------------------");
        System.out.println("     Bienvenido al sistema de Reservas!");

        LocalDate checkInWeather = LocalDate.now();
        LocalTime currentTime = LocalTime.now();

        if (currentTime.isAfter(LocalTime.of(12, 0))) {
            checkInWeather = checkInWeather.plusDays(1);
        }

        Instant checkInWeatherInstant = checkInWeather.atStartOfDay(ZoneId.systemDefault()).toInstant();

        String chosenCountry = selectCountry();
        String checkOutDate = reservationDate("check-out", checkInWeatherInstant);

        String checkinWeather = formatInstant(checkInWeatherInstant);


        System.out.println("Localización elegida: " + chosenCountry);

        commandSet.WeatherDataQuery(chosenCountry, checkinWeather, checkOutDate);

        commandSet.HotelDataQuery(chosenCountry, checkinWeather, checkOutDate);

        System.out.println("        Disfrute de sus vacaciones!");

    }





    private  String reservationDate(String type, Instant checkInWeather) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Seleccione el " + type + ":" );
        System.out.println("1. Para mañana");
        System.out.println("2. En 2 dias");
        System.out.println("3. En 3 dias");
        System.out.println("4. En 4 dias");

        int option = UserSelection();

        Instant reservationDate;
        switch (option) {
            case 1:
                reservationDate = checkInWeather.plus(1, ChronoUnit.DAYS);
                break;
            case 2:
                reservationDate = checkInWeather.plus(2, ChronoUnit.DAYS);
                break;
            case 3:
                reservationDate = checkInWeather.plus(3, ChronoUnit.DAYS);
                break;
            case 4:
                reservationDate = checkInWeather.plus(4, ChronoUnit.DAYS);
                break;
            default:
                System.out.println("Opción inválida.Se selecciona hoy por defecto");
                reservationDate = checkInWeather;
        }

        String formattedDate = formatInstant(reservationDate);
        System.out.println("Check-out seleccionado: " + formattedDate);

        return formattedDate;
    }


    private  int UserSelection() {
        System.out.print("Introduce el número: ");
        Scanner scanner = new Scanner(System.in);
        return scanner.nextInt();
    }

    private String selectCountry() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Selecciona la localización de España que quieres visitar:");
        System.out.println("1. Madrid");
        System.out.println("2. Barcelona");
        System.out.println("3. Sevilla");
        System.out.println("4. Valencia");
        System.out.println("5. Vigo");
        System.out.println("6. Cádiz");
        System.out.println("7. Pamplona");
        System.out.println("8. Málaga");

        int option = UserSelection();

        switch (option) {
            case 1:
                return "Madrid";
            case 2:
                return "Barcelona";
            case 3:
                return "Sevilla";
            case 4:
                return "Valencia";
            case 5:
                return "Vigo";
            case 6:
                return "Cadiz";
            case 7:
                return "Pamplona";
            case 8:
                return "Malaga";
            default:
                System.out.println("Opción invalida.Se selecciona Madrid por defecto");
                return "Madrid";
        }
    }

    private String formatInstant(Instant instant) {
        LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
        return localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

}