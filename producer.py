import aiohttp
from bs4 import BeautifulSoup
import asyncio
import pika
import logging
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = "links_queue"


async def fetch_html(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


def parse_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag['href']
        if href.startswith('/'):
            href = base_url + href
        elif not href.startswith(base_url):
            continue
        links.append((a_tag.text.strip(), href))
    return links


def send_to_queue(links):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    for text, link in links:
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
        logging.info(f'Отправлено:{text} -> {link}')

    connection.close()


async def main(url):
    html = await fetch_html(url)
    links = parse_links(html, url)
    send_to_queue(links)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python producer.py <URL>")
        sys.exit(1)
    url = sys.argv[1]
    asyncio.run(main(url))
