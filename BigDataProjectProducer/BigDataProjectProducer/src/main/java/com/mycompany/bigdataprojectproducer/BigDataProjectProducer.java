package com.mycompany.bigdataprojectproducer;
import com.fasterxml.jackson.databind.ObjectMapper;//to map objects to JSON strings (ka2no JSON fsctory)
import java.io.BufferedReader;//for reading the csv file line by line
import java.io.FileReader;//for opening the csv file
import java.util.LinkedHashMap;// by7afez 3al insertion order fe JSON
import java.util.Map;//key,value
import java.util.Properties;// for configuration of kafka producer. it inherits from Hashtable, ya3ni key, value 
import org.apache.kafka.clients.producer.KafkaProducer;//producer object for sending data
//KafkaProducer<K,V> indicate type for keys and values both serialized into bytes by serializers that i will configure
//Kafka stores bytes only (byte array)
import org.apache.kafka.clients.producer.ProducerRecord;//single producer message (key, value, topic)
//
public class BigDataProjectProducer {
    public static void main(String[] args) {
        String csvPath ="C:\\project\\accidents_dataset.csv";
        String topic = "accidents-raw"; //accidents-rawjson EL 7A2I2I
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); //kafka brokers for the producer to find the cluster
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//turns strings to bytes for keys
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//same thing for values
        props.put("acks", "all");//acknowledgments

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);// <String,String> are generics meaning that key and value are of type String
        ObjectMapper mapper = new ObjectMapper();//gonna use it to serialize the LinkedHashMap into a JSON String

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {

            String line;

            line = br.readLine(); // Skip header (column names) // this returns null when EOF is reached 'end of file'
            if (line == null) {
                System.out.println("Empty CSV!");
                return; //exits main
            }

            int count = 0; //message key in kafka
            int batchCount = 0; // counter for each 1000 batch

            while ((line = br.readLine()) != null) {

                String[] cols = line.split(",", -1);

                if (cols.length != 20) {
                    System.out.println("Skipped malformed row");
                    continue;
                }

                Map<String, Object> json = new LinkedHashMap<>(); //linked hash map keeps insertion order as code
                //key is string and value is object
                json.put("ID", cols[0].trim()); //json.put(key,value)
                json.put("Country", cols[1].trim());
                json.put("Year", cols[2].trim());
                json.put("Month", cols[3].trim());
                json.put("Day_of_Week", cols[4].trim());
                json.put("Time_of_Day", cols[5].trim());
                json.put("Urban_Rural", cols[6].trim());
                json.put("Road_Type", cols[7].trim());
                json.put("Weather_Conditions", cols[8].trim());
                json.put("Visibility_Level", cols[9].trim());
                json.put("Speed_Limit", cols[10].trim());
                json.put("Driver_Alcohol_Level", cols[11].trim());
                json.put("Driver_Fatigue", cols[12].trim());
                json.put("Vehicle_Condition", cols[13].trim());
                json.put("Accident_Severity", cols[14].trim());
                json.put("Traffic_Volume", cols[15].trim());
                json.put("Road_Condition", cols[16].trim());
                json.put("Accident_Cause", cols[17].trim());
                json.put("Region", cols[18].trim());
                json.put("Vehicle_Type", cols[19].trim());
                String jsonString = mapper.writeValueAsString(json); //serializes the map 'json' into a JSON string

                producer.send(new ProducerRecord<>(topic, String.valueOf(count), jsonString));
                //ProducerRecord(String topic, K key, V value) constructor
                //topic is the dest topic name, and count is the message key but we turn it to string using String.valueOf
                // jsonString is the payload (actual data)
                count++; //total messages
                batchCount++;//messages since last sleep

                // === SEND 1000 THEN SLEEP ===
                if (batchCount == 1000) {
                    System.out.println("Sent " + count + " records... Sleeping 5 seconds...");
                    producer.flush();
                    //flush() forces any buffered data to be written to its target destination
                    Thread.sleep(5000); // sleep 5 seconds so we dont overload the broker/network
                    batchCount = 0; // reset batch counter
                }
            }

            producer.flush();//ensuring all remaining messages are sent
            System.out.println("Done! Sent " + count + " records to Kafka topic: " + topic);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close(); //shuts producer down, flushes, and releases resources
        }
    }

}
