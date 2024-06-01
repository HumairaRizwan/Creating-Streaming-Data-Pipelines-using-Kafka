# Creating-Streaming-Data-Pipelines-using-Kafka
## Scenario
You are a data engineer at a data analytics consulting company. You have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. As a vehicle passes a toll plaza, the vehicle’s data like vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to Kafka. Your job is to create a data pipe line that collects the streaming data and loads it into a database.

## Objectives
create a streaming data pipe by performing these steps:

   <ul>
        <li>Start a MySQL Database server.</li>
        <li>Create a table to hold the toll data.</li>
        <li>Start the Kafka server.</li>
        <li>Install the Kafka Python driver.</li>
        <li>Install the MySQL Python driver.</li>
        <li>Create a topic named <code>toll</code> in Kafka.</li>
        <li>Download the streaming data generator program.</li>
        <li>Customize the generator program to stream data to the <code>toll</code> topic.</li>
        <li>Download and customize the streaming data consumer.</li>
        <li>Customize the consumer program to write data into the MySQL database table.</li>
        <li>Verify that the streamed data is being collected in the database table.</li>
    </ul>

