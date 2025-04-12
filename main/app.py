import os
from dotenv import load_dotenv
from flask import Flask

from main.kafka.KafkaConsumer import KafkaConsumer
from main.model.TOPIC import TOPIC

app = Flask(__name__)
load_dotenv()

host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
port = int(os.getenv("FLASK_RUN_PORT", 5000))

vectorConsumer = KafkaConsumer()
vectorConsumer.start("ai-request-for-vector-data", "vector-consumer-group");

if __name__ == '__main__':
    app.run(host=host, port=port, debug=True)

