import os
import time
from google.cloud import pubsub_v1

if __name__ == "__main__":

    # name of the gcloud project
    project = 'noname-12-2021'

    # topic created to ingest iot data
    pubsub_topic = 'projects/noname-12-2021/topics/demo-topic'

    # key linked to the service account which is a background user which is going to acknowledge the sent messages
    path_service_account = 'noname-key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account  # this step will connect to gcp


    ## Steps above is to set the connections etc. below is an example where an input file is read which acts as a publisher of messages.
    ## The subscriber created in GCP can then read the sent messages from the script "subscribe.py".

    # Replace 'my-input-file-path' with your input file path
    input_file = '../example.csv'  ## JUST AN EXAMPLE

    # create publisher instance
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as f:
        # skip header
        header = f.readline()

        # loop over each record
        for line in f:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1) # tries to mimic a data stream every second
