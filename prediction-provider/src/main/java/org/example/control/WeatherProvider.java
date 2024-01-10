package org.example.control;
import org.example.model.*;
import java.time.Instant;

public interface WeatherProvider {
    Weather getWeather(Location location,Instant instant);
}
