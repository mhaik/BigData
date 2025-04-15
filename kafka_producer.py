import json
import random
import sys
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import streamlit as st


from kafka import KafkaProducer
from kafka import KafkaConsumer

def send_data(df, producer):
    single_row  = df.sample(n=1)
    message = single_row.to_dict('records')
    print(message)

    producer.send("lolRankedIn",message[0])

if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    st.title("Kafka")
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(2, 7, 0),
    )
    consumer = KafkaConsumer(
        "lolRankedOut",
        bootstrap_servers = [server],
        api_version=(2, 7, 0),
    )

    df = pd.read_csv("high_diamond_ranked_10min.csv")

    send_button = st.button("Send Sample",on_click=\
        lambda  : send_data(df,producer))

    prediction = 0
    probability = 0

    def read_data():
        try:
            for i in range(5):

                for msg in consumer:
                    value = json.loads(msg.value)
                    print(value)
                    prediction = int(value['prediction'])
                    probability = value['probability']['values'][prediction]
        except KeyboardInterrupt:
            producer.close()
    st.button("read",on_click=read_data)

    st.write(pd.DataFrame([[prediction,probability]]))
