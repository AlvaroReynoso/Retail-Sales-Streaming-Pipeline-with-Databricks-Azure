import uuid
import random
import time
import json
import os
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener la connection string desde las variables de entorno
event_hub_connection_str = os.getenv('EVENT_HUB_CONNECTION_STRING')

if not event_hub_connection_str:
    raise ValueError("EVENT_HUB_CONNECTION_STRING no encontrada en las variables de entorno")

producer = EventHubProducerClient.from_connection_string(conn_str=event_hub_connection_str)

try:
    products = ["P001", "P002", "P003", "P004", "P005"]
    sucursales = ["SucursalA", "SucursalB", "SucursalC"]
    customers = ["CUST001", "CUST002", "CUST003", "CUST004", "CUST005"]
    channels = ["POS", "Online", "SelfCheckout"]

    while True:
        event = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "product_id": random.choice(products),
            "store": random.choice(sucursales),
            "customer_id": random.choice(customers),
            "amount": round(random.uniform(10, 500), 2),
            "channel": random.choice(channels)
        }
        
        # Convertir el evento a JSON string para enviar
        event_data = EventData(json.dumps(event))
        
        # Crear batch y agregar el evento
        event_data_batch = producer.create_batch()
        event_data_batch.add(event_data)
        
        # Enviar el batch (contiene un solo evento)
        producer.send_batch(event_data_batch)
        
        print(f"✅ Evento enviado: {event}")
        time.sleep(10)

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    producer.close()

