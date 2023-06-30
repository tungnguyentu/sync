from bs4 import BeautifulSoup
from imap_tools import MailBox
from typing import List, Optional
from dataclasses import dataclass, asdict
from config import host
from datetime import date, datetime
import time
from functools import wraps
from typing import Callable, List, Tuple, Type
import logging

@dataclass
class MessageAppend:
    user: str
    mailbox: str
    uids: List[int]
    sender: str
    to: str
    event: str = "MessageAppend"
    event_timestamp: Optional[int] = 0
    uidvalidity: Optional[int] = 0
    snippet: Optional[str] = ""
    subject: Optional[str] = ""
    msgid: Optional[str] = ""


def format_addresses(addresses):
    addrs = []
    for addr in addresses:
        try:
            email = addr.get("email")
        except AttributeError:
            email = addr.email
        if email is None:
            continue
        addrs.append(email)
    return ",".join(addrs)


def get_plaintext_snippet(text):
    SNIPPET_LENGTH = 191
    text = " ".join(text.split())
    return text.rstrip()[:SNIPPET_LENGTH]


def remove_tags(text):
    normalized_data = text.replace("\r\n", "\n").replace("\r", "\n")
    clean_text = BeautifulSoup(normalized_data, "lxml").text
    return clean_text


def get_html_snippet(text):
    text = remove_tags(text)
    return get_plaintext_snippet(text)


def synchronous_retry(
    delay=1,
    retries=10,
    exceptions=Exception,
    logger=None
):
    if not logger:
        logger = logging.getLogger(__name__)

    def retry_decorator(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    message = (
                        "Retry exception thrown when attempting to run {}, "
                        "attempt {} of {}".format(f, attempt, retries)
                    )
                    logger.warning(message)
                    attempt += 1
                    time.sleep(delay)
            return f(*args, **kwargs)

        return f_retry

    return retry_decorator


def datetime_json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


def calculate_pages(email, password, folder_name, page_len=2):
    with MailBox(host).login(email, password) as imap:
        criteria = 'ALL'
        imap.folder.set(folder_name)
        found_nums = imap.numbers(criteria)
        pages = int(len(found_nums) // page_len) + \
                1 if len(found_nums) % page_len else int(
                    len(found_nums) // page_len)
        return pages


@synchronous_retry()
def get_events(pages, email, password, folder_name, page_len=2):
    criteria = 'ALL'
    results = []
    for page in pages:
        with MailBox(host).login(email, password) as imap:
            imap.folder.set(folder_name)
            page_limit = slice(page * page_len, page * page_len + page_len)
            for msg in imap.fetch(criteria, bulk=True, limit=page_limit, mark_seen=False, charset='UTF-8'):
                if msg.html:
                    snippet = get_html_snippet(msg.html)
                elif msg.text:
                    snippet = get_plaintext_snippet(msg.text)
                message_append = MessageAppend(
                    user=email,
                    mailbox=folder_name,
                    uids=[int(msg.uid)],
                    sender=msg.from_values.email,
                    to=format_addresses(msg.to_values),
                    snippet=snippet
                )
                for key, value in msg.headers.items():
                    if key.lower() == "message-id":
                        message_append.msgid,  = value
                results.append(message_append)
    return results