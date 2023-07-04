import json
from typing import List
import src.utils as utils
import ssl
from src.publisher import KafkaPublisher
from config import *
from dataclasses import asdict
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


async def process_folder(publisher: KafkaPublisher, email: str, password: str, folder: str):
    logging.info(f"Processing folder: {folder}, user: {email}")
    pages = utils.calculate_pages(
        email=email,
        password=password,
        folder_name=folder,
        page_len=200
    )
    pages = list(range(pages))
    for i in range(0, len(pages), 2):
        percent = (len(pages[i: i + 2]) / len(pages)) * 100
        if percent % 10 == 0:
            utils.send_to_telegram(f"email: {email}, folder: {folder}, percent: {percent}")
        events = utils.get_events(pages[i: i + 2], email, password, folder, page_len=200)
        for event in events:
            await publisher.publish(event_type="MessageAppend", key=email, payload=asdict(event))


async def process_account(publisher: KafkaPublisher, email: str, password: str, folders: List[str], ratio):
    utils.send_to_telegram(f"email: {email}, ratio: {ratio}")
    tasks = []
    for folder in folders:
        if folder == "WEBMAIL_SCHEDULED":
            continue
        task = asyncio.create_task(process_folder(publisher, email, password, folder), name=folder)
        logging.info(f"FOLDER: {folder}")
        tasks.append(task)
    await asyncio.gather(*tasks)


async def main(publisher: KafkaPublisher):
    with open("data.txt", "r") as f:
        data = json.loads(f.readline())
    for i in range(0, len(data), 5):
        tasks = []
        for info in data[i: i + 5]:
            email = info["email"]
            ratio = f"{data.index(info) + 1}/{len(data)}"
            password = info["password"]
            folders = info["folders"]
            task = asyncio.create_task(process_account(publisher, email, password, folders, ratio), name=email)
            logging.info(f"USER: {email}")
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == "__main__":
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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(publisher))
    loop.close()
