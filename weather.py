import requests
from confluent_kafka import Producer
import json

KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'weather_data' 

API_KEY = 'ea9b6d901f5b485e67dcea216e73e676'  
CITY = 'London'  
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

def fetch_weather_data(city: str):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'  
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching weather data: {response.status_code}")


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def main():
    try:
        weather_data = fetch_weather_data('Bangalore')
        weather_message = json.dumps(weather_data)

        producer.produce(KAFKA_TOPIC, value=weather_message, callback=delivery_report)
        producer.flush()
        print(f"Weather data sent to Kafka topic '{KAFKA_TOPIC}'")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
