import src.utils as utils
import ssl
from src.publisher import KafkaPublisher
from config import *
from dataclasses import asdict

def main():
    context = None
    if kafka_ssl_context:
        context = ssl.create_default_context()
        context.options &= ssl.OP_NO_TLSv1
        context.options &= ssl.OP_NO_TLSv1_1
    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=event_topic,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        ssl_context=context,
    )
    publisher.connect_publisher()
    accounts = {}
    folders = ["INBOX"]
    for email, password in accounts.items():
        print(email)
        for folder in folders:
            pages = utils.calculate_pages(
                email=email,
                password=password,
                folder_name=folder,
                page_len=50
            )
            pages = list(range(pages))
            for i in range(0, len(pages), 1):
                events = utils.get_events(pages[i: i + 1], email, password, folder, page_len=50)
                for event in events:
                    publisher.publish(event_type="MessageAppend", key=email, payload=asdict(event))
            break
        break

if __name__ == "__main__":
    main()
