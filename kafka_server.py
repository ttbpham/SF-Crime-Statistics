import producer_server
from pathlib import Path

def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="gov.department.police.sf.crime",
        bootstrap_servers= "localhost:9093" ,
        client_id=None
    )
    print(f"initiate producer")
    return producer


def feed():

    producer = run_kafka_server()

    print("start generate data")
    try:
        producer.generate_data()
    
    except:
        print(f"error generating data")
        producer.close()
        
    print("end generate data")

if __name__ == "__main__":
    print(f"start producer server")
    feed()
