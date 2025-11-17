from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta
import json
from street_view_utils import generate_and_upload_random_image
import os
import random
import uuid
import time
import simplejson as json

random.seed(123)

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate the increments for lattitude and longitude
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] 
                            - LONDON_COORDINATES["latitude"]) / 100

LONGTITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] 
                            - LONDON_COORDINATES["longitude"]) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_topic")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_topic")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_topic")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_topic")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_topic")

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type,
    }
    
def generate_weather_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature_celsius': round(random.uniform(24, 34), 1), 
        'humidity_percent': random.randint(60, 90),
        'condition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Thunderstorm']),
        'precipitation_mm': round(random.uniform(0, 50), 1),
        'wind_speed_kmh': round(random.uniform(0, 50), 1),
        'air_quality_index': random.randint(0, 150),
    }
    
def generate_emergency_incident_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'type': random.choice(['Accident', 'Roadblock', 'Medical Emergency']),
        'severity': random.choice(['Low', 'Medium', 'High']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Lorem ipsum dolor sit amet consectetur adipiscing elit. Consectetur adipiscing elit quisque faucibus ex sapien vitae. Ex sapien vitae pellentesque sem placerat in id. Placerat in id cursus mi pretium tellus duis. Pretium tellus duis convallis tempus leo eu aenean.',
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    snapshot_url = generate_and_upload_random_image()
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'camera_id': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': snapshot_url,
    }

def simulate_vehicle_movement():
    global start_location
    
    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGTITUDE_INCREMENT
    
    # random road journey
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return start_location
    

def generate_vehicle_data(device_id):
    
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'Toyota',
        'model': 'Wigo',
        'year': 2025,
        'fuel_type': 'Diesel',
    }
    
def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")  
        
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        
        
def produce_to_kafka(producer, topic, data):
    
    producer.produce(
        topic=topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
        )
    
    producer.flush()
    
def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)        
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_cam_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='Hanabishi-002')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])  
        
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and
            vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print("Vehicle has reached Birmingham. Ending simulation.")
            break
        
        produce_to_kafka(producer,VEHICLE_TOPIC, vehicle_data)
        produce_to_kafka(producer,GPS_TOPIC, gps_data)
        produce_to_kafka(producer,TRAFFIC_TOPIC, traffic_cam_data)
        produce_to_kafka(producer,WEATHER_TOPIC, weather_data)
        produce_to_kafka(producer,EMERGENCY_TOPIC, emergency_incident_data)
        
        time.sleep(5)
        
if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Error: {err}"),
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle1')

    except KeyboardInterrupt:
        print('Simulation interrupted by user.')
    except Exception as e:
        print(f'An error occurred: {e}')