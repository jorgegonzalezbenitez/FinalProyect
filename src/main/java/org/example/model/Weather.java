package org.example.model;
import java.time.Instant;
public class Weather {
    private final double temp;
    private final int humidity;
    private final int all;
    private final double speed;
    private final Double pop;
    private final Instant dt;

    public Weather(double temp, int humidity, int all, double speed, Double pop, Instant dt) {
        this.temp = temp;
        this.humidity = humidity;
        this.all = all;
        this.speed = speed;
        this.pop = pop;
        this.dt = dt;
    }

    public double getTemp() {
        return temp;
    }

    public int getHumidity() {
        return humidity;
    }

    public int getAll() {
        return all;
    }

    public double getSpeed() {
        return speed;
    }

    public Double getPop() {
        return pop;
    }

    public Instant getDt() {
        return dt;
    }
}

