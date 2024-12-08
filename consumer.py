import aiohttp
from bs4 import BeautifulSoup
import asyncio
import pika
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = "links_queue"
TIMEOUT = 60

# Хранилище для отслеживания обработанных ссылок
processed_links = set()


async def fetch_html(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


def parse_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string if soup.title else "No Title"
    logging.info(f"Обработка страницы: {title} ({base_url})")

    links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag['href']
        if href.startswith('/'):
            href = base_url + href
        elif not href.startswith(base_url):
            continue
        links.append((a_tag.text.strip(), href))
    return links


async def process_link(link, channel):
    html = await fetch_html(link)
    base_url = '/'.join(link.split('/')[:3])
    links = parse_links(html, base_url)

    for text, href in links:
        if href not in processed_links:
            processed_links.add(href)
            channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=href)
            logging.info(f"В очереди: {text} -> {href}")


async def consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    try:
        while True:
            method_frame, header_frame, body = channel.basic_get(QUEUE_NAME)
            if body:
                link = body.decode()
                if link not in processed_links:  # Проверяем, была ли ссылка обработана
                    processed_links.add(link)  # Добавляем ссылку в множество
                    await process_link(link, channel)
                    channel.basic_ack(method_frame.delivery_tag)
                else:
                    logging.info(f"Пропускаем уже обработанные ссылки: {link}")
                    channel.basic_ack(method_frame.delivery_tag)
            else:
                await asyncio.sleep(TIMEOUT)
                logging.info("Очередь пустая, выходим...")
                break
    finally:
        logging.info('Завершение работы')
        connection.close()


if __name__ == "__main__":
    asyncio.run(consumer())
