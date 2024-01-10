package org.example;

import org.example.Controller.*;
import org.example.Model.*;
import org.example.View.*;

import javax.jms.JMSException;
import java.util.concurrent.TimeUnit;

public class HolidayMain {


    public static void main(String[] args) throws JMSException {
        Consumer subscriber = new RealTimeConsumer(args[0]);
        Datamart databaseStore = new Datamart(args[1]);
        subscriber.start(databaseStore);
        try{
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        CommandSet commandset = new Command(args[1]);
        UserInterface userInterface = new UserInterface(commandset);
        userInterface.execute();

    }
}