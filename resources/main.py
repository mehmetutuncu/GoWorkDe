import json
import logging
import requests

from bs4 import BeautifulSoup
from peewee import IntegrityError

from models import db, TableMailDB, GoWorkDe
from time import sleep
from threading import Thread

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_soup(content, features):
    return BeautifulSoup(markup=content, features=features)


def extract_companies(soup):
    company_url_items = soup.select('div.company-card h3.company-card__title a')
    return [f"{base_url}{company_url_item.get('href')}" for company_url_item in company_url_items]


def get_companies_from_page(page, retry=0):
    if retry > 3:
        logging.error(f"Page fetching error from {search_url} - Page: {page}")
        return []
    params = {
        "page": page
    }
    response = requests.get(url=search_url, params=params)
    if response.status_code == 200 and response.ok:
        soup = get_soup(content=response.content, features='lxml')
        return extract_companies(soup=soup)
    else:
        sleep(60)
        return get_companies_from_page(page=page, retry=retry + 1)


def get_website(soup):
    website = soup.select_one('div.company-header__web-page span')
    return website.get('data-href') if website else ''


def get_company_name(soup):
    company_name = soup.select_one('h2.company-header__title')
    return company_name.text if company_name else ''


def get_email(soup):
    email = soup.select_one('a.__cf_email__')
    if email:
        cf_email = email.get('data-cfemail')
        encoded_bytes = bytes.fromhex(cf_email)
        return bytes(byte ^ encoded_bytes[0] for byte in encoded_bytes[1:]).decode('utf-8')
    return None


def get_phone_and_rating(soup):
    application_json = soup.select_one('script[type="application/ld+json"]')
    if application_json:
        json_data = json.loads(application_json.text)
        phone = json_data.get('itemReviewed', {}).get('telephone', '')
        rating_value = json_data.get('ratingValue', '0')
        rating_count = json_data.get('ratingCount', '0')
        return phone, rating_count, rating_value
    return '', '', ''


def save_company(**kwargs):
    try:
        instance = GoWorkDe(**kwargs)
        instance.save()
    except IntegrityError:
        return


def extract_company_data(company_url, retry=0):
    if retry > 3:
        logging.error(f"Company fetching error from {company_url}")
        return None

    response = requests.get(url=company_url)
    if response.status_code == 200 and response.ok:
        soup = get_soup(content=response.content, features='lxml')
        website = get_website(soup=soup)
        company_name = get_company_name(soup=soup)
        email = get_email(soup=soup)
        if not email or TableMailDB.select().where(TableMailDB.email == email).exists():
            return
        phone, rating_count, rating_value = get_phone_and_rating(soup=soup)
        data = dict(phone=phone, rating_count=rating_count, rating_value=rating_value, company_name=company_name,
                    email=email, website=website, company_url=company_url)
        save_company(**data)
    else:
        sleep(60)
        return extract_company_data(company_url=company_url, retry=retry + 1)


def pagination_section(page=1):
    logging.info(f"Page {page} is fetching.")
    companies = get_companies_from_page(page=page)

    if not companies:
        return

    threads = []
    for company in companies:
        process = Thread(target=extract_company_data, args=(company,), daemon=True)
        process.start()
        threads.append(process)

        if len(threads) % 100 == 0:
            [thread.join() for thread in threads]
            threads = []

    [thread.join() for thread in threads]

    return pagination_section(page=page + 1)


if __name__ == '__main__':
    base_url = "https://gowork.de"
    search_url = "https://gowork.de/search"
    if db.is_closed():
        db.connect()
    db.create_tables([GoWorkDe])
    pagination_section()
