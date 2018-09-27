import requests
from bs4 import BeautifulSoup
import csv
import psycopg2
from time import sleep
from config_avito import config_dbsql


def get_html(url):
    header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
    r = requests.get(url, headers=header)
    return r.text

def get_total_pages(html):
    soup = BeautifulSoup(html, 'html.parser')

    pages = soup.find('div', class_='pagination-pages').find_all('a', class_='pagination-page')[-1].get('href')
    total_pages = pages.split('=')[1].split('&')[0]

    return int(total_pages)+1

def write_csv(data):
    with open('avito.csv', 'a') as f:
        writer = csv.writer(f)

        writer.writerow((data['title'], data['price'], data['text'], data['url']))

def write_sql(data):
    try:
        conn = psycopg2.connect(config_dbsql())
        #CREATE TABLE avito (id serial PRIMARY KEY, price text, title text, url text, text text)
        cur = conn.cursor()
        print("Success connect to PostgreSQL!")
        print(data)
        cur.execute("INSERT INTO avito (price, title, url, text) VALUES (%s, %s, %s, %s)", (data['price'], data['title'], data['url'], data['text']))
    except:
        print("Can't connect to PostgreSQL!")
    conn.commit()
    cur.close()
    conn.close()

def get_page_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    sleep(2)
    ads = soup.find('div', class_='catalog-content').find_all('div', class_='item_table_extended')
    
    for ad in ads:
        try:
            title = ad.find('div', class_='description').find('h3').text.strip()
        except:
            title = ''
        
        try:
            url = 'https://www.avito.ru' 
            url = url + ad.find('h3').find('a', class_='item-description-title-link').get('href')
        except:
            url = ''

        try:
            price = ad.find('span', class_='price').text.strip()
        except:
            price = ''

        try:
            text = ad.find('div', class_='specific-params').text.strip()
        except:
            text = ''

        data = {'title': title,
                'price': price,
                'text': text,
                'url': url}
        write_sql(data)

def main():
    url = 'https://www.avito.ru/moskva/avtomobili?p=1&radius=200&i=1'
    base_url = 'https://www.avito.ru/moskva/avtomobili?'
    page_part = 'p='
    query_part = '&radius=200&i=1'

    total_pages = get_total_pages(get_html(url))

    for i in range(1, 500):
        url_gen = base_url + page_part + str(i) + query_part
        html = get_html(url_gen)
        get_page_data(html)


if __name__ == '__main__':
    main()