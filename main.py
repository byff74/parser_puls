import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import time
import schedule


log_format = '%(asctime)s - %(levelname)s - %(message)s'
log_encoding = 'utf-8'


error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(
    'parsing.log', encoding=log_encoding)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(log_format))
error_logger.addHandler(error_handler)


info_logger = logging.getLogger('info_logger')
info_logger.setLevel(logging.INFO)
info_handler = logging.FileHandler(
    'parsing.log', encoding=log_encoding)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter(log_format))
info_logger.addHandler(info_handler)


def parsing_puls():
    with sqlite3.connect("reviews.db") as con:
        con.execute('''CREATE TABLE IF NOT EXISTS reviews
                                (ID INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, convert_rating_review TEXT, date TEXT, feedback TEXT)''')
    base_url = "https://www.amk-grupp.ru/reviews"
    list_reviews = {"rate5": "5",
                    "rate4": "4",
                    "rate3": "3",
                    "rate2": "2",
                    "rate1": "1",
                    "0": "Нет рейтинга"
                    }
    start_page = 1
    num_pages = 1
    for page in range(start_page, start_page + num_pages):
        url = f'{base_url}?page={page}'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        list_review = soup.find_all(
            'div', class_='reviews-item js-reviews-review')

        for review in list_review:
            feedback = review.select_one('.text-').text.strip()
            rating_review = review.select_one('span.r-review-rating')
            if rating_review is not None:
                class_name = rating_review.get('class')[1]
            else:
                class_name = '0'
                error_logger.error(
                    'Не могу спарсить рейтинг! [Не найден класс рейтинга]')
            name = review.select_one(".r-name-").text.strip()
            date = review.select_one(".r-date-").text.strip()
            if class_name in list_reviews:
                convert_rating_review = list_reviews[class_name]
                con.execute("INSERT OR REPLACE INTO reviews (name, convert_rating_review, date, feedback) VALUES (?, ?, ?, ?)",
                            (name, convert_rating_review, date, feedback))
            con.commit()



def parse_and_schedule():
    parsing_puls()
    info_logger.info('Парсер запущен!')

schedule.every(5).seconds.do(parse_and_schedule)

while True:
    schedule.run_pending()
    time.sleep(1)