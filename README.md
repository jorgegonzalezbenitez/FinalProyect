# _Second practice_
Desarrollo de Aplicaciones para Ciencia de datos

Curso 2023-2024

2º Grado en Ciencia e Ingeniería de datos

Universidad Las Palmas de Gran Canaria

## _Execution_

To execute the application I pass the following modules as arguments: For the four modules I pass the connection from my computer to ActiveMQ, and in the datalake module I simply pass the root of the directory, that is, the user can locate the directory at your mercy but you cannot change its structure internally. I also wanted to point out that I set the database address for my computer.

## _Functionality_

The software is responsible for acquiring and storing both meteorological data and the availability of hotels in different locations in Spain. It relies on a weather service provider (OpenWeatherMap) and for hotels it uses an API (Xotelo) to obtain the information and send it to the broker that acts as an intermediary, and write the received events in a directory (datalake), and also save it in a database (datamart) to be able to manage information in real time for quick queries. Finally, I implement a user interface using a CLI to interact with the user. This process occurs every 6 hours to update the latest forecasts for both events but the user interface can be used whenever desired.

## _Resources Used_

#### Development Environments

The project is developed in the Intellij development environment. Java is used for the code implementation.

#### Version Control Tools

I use Git as a version control tool. The use of Git makes it easier to make changes in the code development, providing the assurance that such changes cannot be deleted.

#### Documentation Tools

I use Markdown, providing a quick and concise overview of the project.

## Design

#### Design patterns

The application adheres to the Single Responsibility Principle (SRP), ensuring clear responsibilities for each component and enhancing code clarity. It adopts the Controller design pattern from the Model-View-Controller (MVC) architecture, facilitating efficient management of interactions between the model and the view.

In terms of asynchronous communication, the Observer pattern is employed to receive messages from a JMS topic. The AMQTopicSubscriber class serves as an observer, efficiently handling the reception of messages as they become available. This combination of design principles, patterns, and Lambda architecture establishes a robust foundation for the application, fostering modularity, scalability, and maintainability.Regarding database management, the application incorporates the use of the SQLite language to facilitate efficient data handling. The integration of SQLite provides a lightweight and high-performance solution, allowing for effective storage and retrieval of information.

#### Design principle

I strategically apply several design patterns to enhance the overall architecture and maintainability of the system. The system is organized into four key modules, each serving a distinct purpose. Two of these modules, hotel-provider and weather-provider, function as sensors responsible for sending events to the ActiveMQ broker. This setup leverages the Observer pattern, ensuring a decoupled and flexible communication mechanism between the sensor modules and the broker.

On the receiving end, the datalake-builder module plays a crucial role by subscribing to these events through a durable subscriber. The implementation of the Observer pattern here ensures that the datalake-builder efficiently captures and processes events from the sensors. Additionally, the datalake-builder incorporates the Strategy pattern, allowing for flexible and interchangeable algorithms to handle the specific structure and storage of the received messages in the directory.

The final module, holiday-business-unit, not only subscribes to events from both sensors via the broker but also integrates the Model-View-Controller (MVC) architectural style. This pattern provides a clear separation of concerns, enhancing modularity and maintainability. The holiday-business-unit module uses the Command pattern to encapsulate requests as objects, facilitating the handling of various commands related to storing events in a database and updating the user interface. This not only ensures a clean and organized codebase but also simplifies the addition of new features in the future.

Additionally, the application leverages a Lambda system architecture, dividing operations into real-time and batch processing. This architectural choice enables effective handling of large datasets and provides flexibility for handling continuous streams and periodic batch operations.


#### Project Diagram in StarUml

This is the UML class diagram that represents and illustrates the functionality of the "prediction-Provider" module.
<img width="586" alt="image" src="https://github.com/jorgegonzalezbenitez/WeatherApi/assets/145259600/4832f87e-3210-4d3b-b2c1-49eed6a38eae">

This is the UML class diagram that represents and illustrates the functionality of the "hotel-Provider" module.
<img width="567" alt="image" src="https://github.com/jorgegonzalezbenitez/WeatherApi/assets/145259600/edb6e417-d83e-4d14-bf29-f1026a787b24">

This is the UML class diagram that represents and illustrates the functionality of the "datalake-builder" module.
<img width="538" alt="image" src="https://github.com/jorgegonzalezbenitez/WeatherApi/assets/145259600/4dfc25f4-8f43-4b86-80c9-152257c752d2">

This is the UML class diagram that represents and illustrates the functionality of the "holiday-business-unit" module.
<img width="522" alt="image" src="https://github.com/jorgegonzalezbenitez/WeatherApi/assets/145259600/b4a637e8-59ff-42d3-82dc-7e1f1c1709f1">



