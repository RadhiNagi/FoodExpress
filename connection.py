import json
import os
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME")


def send_to_event_hub(order_data):

    try:
        producer = EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        )

        order_json = json.dumps(order_data)

        event_batch = producer.create_batch()
        event_batch.add(EventData(order_json))

        producer.send_batch(event_batch)
        producer.close()

        return "Successfully sent to Event Hub"

    except Exception as e:
        print(f"Error sending to Event Hub: {str(e)}")
        return False
    

