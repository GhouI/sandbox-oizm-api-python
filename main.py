import requests
import pandas as pd
import time
import os

# Credentials and API endpoint
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
api_base_url = "https://sandbox.oizom.com/v1"
token_url = f"{api_base_url}/oauth2/token"
sensor_device_ids = ["YG19P0025", "YG19D0004", "YG19W0001", "YG19O0005"]

# Create a session for making requests
session = requests.Session()

# Generate an authorization token
token_payload = {
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "client_credentials",
    "scope": "view_data"
}
token_response = session.post(token_url, json=token_payload)

if token_response.status_code == 200:
    access_token = token_response.json()["access_token"]
else:
    print(f"Token request failed with status code {token_response.status_code}")
    exit()

# Initialize an empty DataFrame to store all sensor data
combined_data = pd.DataFrame()

# Fetch and concatenate data for each sensor with rate limiting and retries
for device_id in sensor_device_ids:
    sensor_data_url = f"{api_base_url}/data/cur/{device_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "ClientId": client_id,
        "Content-Type": "application/json",
    }

    retries = 0
    while retries < 3:  # Retry up to 3 times if rate limiting occurs
        response = session.get(sensor_data_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            combined_data = combined_data._append(df, ignore_index=True)
            print(f"Data for sensor {device_id} fetched and added to the combined data.")
            break
        elif response.status_code == 429:
            # Rate limited, wait and retry
            print(f"Rate limited for sensor {device_id}. Retrying...")
            time.sleep(5)  # Wait for 5 seconds before retrying
            retries += 1
        else:
            print(f"Data request for sensor {device_id} failed with status code {response.status_code}")
            break

# Save the combined data to a single CSV file
combined_data.to_csv("all_sensor_data.csv", index=False)
print("Combined data for all sensors saved to all_sensor_data.csv")

# Close the session
session.close()
