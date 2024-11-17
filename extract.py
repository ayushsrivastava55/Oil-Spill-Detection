import asyncio
import websockets
import json
from datetime import datetime, timezone
import pandas as pd
import os

async def connect_ais_stream():
    uri = "wss://stream.aisstream.io/v0/stream"
    api_key = "06e84c6b547de2af24bdbf71e3e0f84d6a4fd714"  # Replace with your actual API key

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [[[18.00, -97.50], [30.70, -81.10]]],  # Required!
        # "FiltersShipMMSI": ["368207620", "367719770", "211476060", ""],  # Optional!
        "FilterMessageTypes": ["PositionReport"]  # Optional!
    }

    csv_file = "ship_positions.csv"
    
    # Check if the file already exists
    file_exists = os.path.isfile(csv_file)

    try:
        async with websockets.connect(uri) as websocket:
            subscribe_message_json = json.dumps(subscribe_message)
            await websocket.send(subscribe_message_json)
            print("Subscription message sent.")

            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message.get("MessageType")

                if message_type == "PositionReport":
                    ais_message = message['Message'].get('PositionReport', {})
                    ship_id = ais_message.get('UserID', 'N/A')
                    latitude = ais_message.get('Latitude', 'N/A')
                    longitude = ais_message.get('Longitude', 'N/A')
                    Navigational_status = ais_message.get('NavigationalStatus', 'N/A')
                    timestamp = datetime.now(timezone.utc)

                    # Prepare the data to be written
                    data = {
                        "timestamp": [timestamp],
                        "ship_id": [ship_id],
                        "latitude": [latitude],
                        "longitude": [longitude],
                        "Navigational Status": [Navigational_status]
                    }

                    df = pd.DataFrame(data)

                    # Append the data to the CSV file
                    df.to_csv(csv_file, mode='a', header=not file_exists, index=False)

                    # Print the latest entry
                    print(f"[{timestamp}] ShipId: {ship_id} Latitude: {latitude} Longitude: {longitude}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":  # Corrected this to "__main__"
    asyncio.run(connect_ais_stream())