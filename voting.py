import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaError, SerializingProducer

from main import delivery_report  # make sure this function is defined in main.py

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
}

# Kafka Consumer for voters_topic
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

# Kafka Producer for votes_topic
producer = SerializingProducer(conf)

def fetch_candidates(cur):
    cur.execute("""
        SELECT row_to_json(c)
        FROM candidates c;
    """)
    candidates_raw = cur.fetchall()
    candidates = [c[0] for c in candidates_raw]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    return candidates

def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch candidates from DB once
    candidates = fetch_candidates(cur)
    print("Candidates loaded:", candidates)

    # Subscribe consumer to voters topic
    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Consumer error: ", msg.error())
                    break

            # Decode voter message
            voter = json.loads(msg.value().decode('utf-8'))

            # Randomly choose candidate
            chosen_candidate = random.choice(candidates)

            # Compose vote dictionary
            vote = {
                **voter,
                **chosen_candidate,
                "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "vote": 1
            }

            try:
                print(f"User {vote['voter_id']} is voting for candidate {vote['candidate_id']}")

                # Insert vote into DB
                cur.execute("""
                    INSERT INTO votes (voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)
                """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                conn.commit()

                # Produce vote message to Kafka votes_topic
                producer.produce(
                    'votes_topic',
                    key=str(vote["voter_id"]),
                    value=json.dumps(vote),
                    on_delivery=delivery_report
                )
                producer.poll(0)

            except Exception as e:
                print("Error while inserting vote or producing Kafka message:", e)
                conn.rollback()
                continue

            time.sleep(0.2)

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        consumer.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
