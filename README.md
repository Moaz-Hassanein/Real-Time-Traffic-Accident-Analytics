# 🚗 Real-Time Traffic Accident Analytics

## 📌 Overview  
This project builds a real-time big data pipeline for analyzing traffic accidents. It processes streaming accident data to extract insights about location, severity, weather conditions, and key contributing factors such as speeding, distracted driving, and road conditions.

The system helps identify accident hotspots and time-based patterns (daily, weekly, seasonal), along with analyzing vehicle types involved to support better road safety decisions.

## ⚙️ Tech Stack  
- Apache Kafka (real-time data streaming)  
- Apache Spark (real-time data processing & analytics)  
- Database (data storage)  
- Python / Java (integration & processing)

## 🔁 Pipeline Architecture  
- Data is published to Kafka topics in real time  
- Spark consumes streaming data from Kafka  
- Real-time processing and analytics are performed using Spark  
- Processed results are stored in a database  

## 📊 Key Insights  
- Accident hotspot detection (geographical analysis)  
- Time-based accident trends  
- Weather and road condition impact  
- Accident causation factors  
- Vehicle type distribution analysis  

## 🎯 Objective  
To build a scalable real-time analytics system that transforms streaming traffic accident data into actionable insights for improving road safety and decision-making.
