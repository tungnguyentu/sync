import src.utils as utils
import ssl
from src.publisher import KafkaPublisher
from config import *


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
    folders = []
    for email, password in accounts.items():
        for folder in folders:
            pages = utils.calculate_pages(
                email=email,
                password=password,
                folder_name=folder,
                page_len=50
            )
            utils.get_events(publisher, pages, email, password, folder, page_len=50)

if __name__ == "__main__":
    main()
