import argparse
import yaml
import requests
import json
import datetime
import pytz
import warnings
from urllib.parse import urlparse
from urllib3.exceptions import InsecureRequestWarning

# Suppress warnings related to SSL verification
warnings.simplefilter('ignore', InsecureRequestWarning)

class Config:
    def __init__(self, api_url, username, password, account, service_location, extract_days, output_file_usage, interval):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.account = account
        self.service_location = service_location
        self.extract_days = extract_days
        self.output_file_usage = output_file_usage
        self.interval = interval

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

def save_to_json(data, output_file):
    with open(output_file, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=4)

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

    save_to_json(json_data, config.output_file_usage)

if __name__ == "__main__":
    main()
