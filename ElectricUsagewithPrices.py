import argparse
import yaml
import requests
import pytz
import json
import datetime
import warnings
from urllib.parse import urlparse
from urllib3.exceptions import InsecureRequestWarning
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS  # Add this import

# Suppress warnings related to SSL verification
warnings.simplefilter('ignore', InsecureRequestWarning)

class Config:
    def __init__(self, api_url, username, password, account, service_location, extract_days, output_file_usage, price_per_kw, retain_days, influxdb):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.account = account
        self.service_location = service_location
        self.extract_days = extract_days
        self.output_file_usage = output_file_usage
        self.price_per_kw = price_per_kw
        self.retain_days = retain_days
        self.influxdb = influxdb

def load_config(config_file):
    with open(config_file, 'r') as file:
        config_data = yaml.safe_load(file)
        return Config(
            api_url=config_data.get('api_url'),
            username=config_data.get('username'),
            password=config_data.get('password'),
            account=config_data.get('account'),
            service_location=config_data.get('service_location'),
            extract_days=config_data.get('extract_days'),
            output_file_usage=config_data.get('output_file_usage'),
            price_per_kw=config_data.get('price_per_kw'),
            retain_days=config_data.get('retain_days'),
            influxdb=config_data.get('influxdb'),
            interval=config_data.get('interval')
            )

def calculate_date_range(days):
    end_date = datetime.datetime.now().replace(hour=23, minute=59, second=59)
    start_date = end_date - datetime.timedelta(days=days - 1)
    return start_date, end_date

def auth(config):
    client = requests.Session()
    form_data = {
        'userId': config.username,
        'password': config.password
    }
    auth_url = f"{config.api_url}/services/oauth/auth/v2"
    parsed = urlparse(config.api_url)
    authority = parsed.hostname

    headers = {
        'authority': authority
    }

    try:
        response = client.post(auth_url, data=form_data, headers=headers, verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return "", str(e)

    try:
        oauth_data = response.json()
    except json.JSONDecodeError as e:
        return "", str(e)

    authorization_token = oauth_data.get('authorizationToken', '')
    if not authorization_token:
        return "", "auth response did not include auth token"

    return authorization_token, None

def fetch_data(start, end, config, jwt):
    client = requests.Session()
    start_timestamp = int(start.timestamp() * 1000)
    end_timestamp = int(end.timestamp() * 1000)

    url = (f"{config.api_url}/services/secured/readings/graph/{config.service_location}/{config.account}"
           f"?startDateTime={start_timestamp}&endDateTime={end_timestamp}"
           f"&applicationName=CONSUMER&graphUnitOfMeasure=KWH&timeFrame={config.interval}")

    headers = {
        'Authorization': f"Bearer {jwt}",
        'x-nisc-smarthub-username': config.username,
        'Content-Type': 'application/json'
    }

    try:
        response = client.get(url, headers=headers, verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return None, str(e)

    return response.content, None

def convert_timestamp(timestamp_ms):
    timestamp_s = timestamp_ms / 1000
    dt = datetime.datetime.fromtimestamp(timestamp_s, pytz.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def process_data(data, price_per_kw):
    output = []
    for item in data:
        readings = item.get('readings', [])
        
        for reading in readings:
            reads = reading.get('reads', [])
            
            for read in reads:
                interval = read.get('interval', {})
                start_time = interval.get('start', 0)
                metrics = read.get('metrics', {})
                
                start_time_formatted = convert_timestamp(start_time)
                total = metrics.get('total', 0.0) or 0.0
                price = total * price_per_kw
                
                row = {
                    'Start Time': start_time_formatted,
                    'Total': total,
                    'Price': price
                }
                output.append(row)
    
    return output

def save_to_json(data, output_file, retain_days):
    try:
        with open(output_file, 'r') as jsonfile:
            existing_data = json.load(jsonfile)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    current_time = datetime.datetime.now(pytz.utc)
    retention_cutoff = current_time - datetime.timedelta(days=retain_days)

    existing_data_dict = {entry['Start Time']: entry for entry in existing_data}

    for entry in data:
        existing_data_dict[entry['Start Time']] = entry

    filtered_data = [entry for entry in existing_data_dict.values() if parse_datetime(entry['Start Time']) >= retention_cutoff]

    with open(output_file, 'w') as jsonfile:
        json.dump(filtered_data, jsonfile, indent=4)

def parse_datetime(date_str):
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
    except ValueError:
        return None

def write_to_influxdb(data, config):
    client = InfluxDBClient(url=config.influxdb['url'], token=config.influxdb['token'], org=config.influxdb['org'])
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    points = []
    for entry in data:
        point = Point("energy_usage").tag("location", config.service_location).field("total_kwh", entry['Total']).field("price", entry['Price']).time(entry['Start Time'], WritePrecision.NS)
        points.append(point)
    
    write_api.write(bucket=config.influxdb['bucket'], record=points)

def main():
    config = load_config('config.yaml')
    start_date, end_date = calculate_date_range(config.extract_days)
    jwt, error = auth(config)

    if error:
        print(f"Authentication failed: {error}")
        return

    data, error = fetch_data(start_date, end_date, config, jwt)
    if error:
        print(f"Data fetching failed: {error}")
        return

    try:
        json_data = json.loads(data)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    processed_data = process_data(json_data, config.price_per_kw)
    save_to_json(processed_data, config.output_file_usage, config.retain_days)
    write_to_influxdb(processed_data, config)  # Push data to InfluxDB

if __name__ == "__main__":
    main()
