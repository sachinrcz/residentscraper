import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ArtistItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os
import datetime
from scrapy.utils.project import get_project_settings
import logging
from urllib.parse import urljoin


class InstagramSpider(scrapy.Spider):

    name = "InstagramSpider"

    domain = "https://www.instagram.com"

    logger = logging.getLogger("InstagramSpider")

    project_settings = get_project_settings()

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY":2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY" : 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1,
        "SOURCE_ID":project_settings['INSTAGRAM_SOURCE_ID']
        }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        ## Get URLs from SQL
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'],
                             passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()
        query = 'SELECT artistID, url FROM dj_artist_website where sourceID={} LIMIT 1000'.format(
        self.custom_settings.get('INSTAGRAM_SOURCE_ID'))
        results = cursor.execute(query)
        rows = cursor.fetchall()
        urls = [row[1].strip() for row in rows]
        for url in urls:
            url = url.replace('http://', 'https://').strip()
            if '/tags/' in url:
                continue
            if 'https' in url and 'instagram.com' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
            else:
                if len(url) > 6 and 'instagram.com' in url:
                    request = scrapy.Request(url='https://' + url, callback=self.parse)
                    yield request

        ## Test Single URL
        # url = 'https://www.instagram.com/joachimgarraud'
        # request = scrapy.Request(url=url, callback=self.parse)
        # yield request

    def parse(self,response):
        item = ArtistItem()
        for field in item.fields:
            item.setdefault(field, '')

        item['sourceURL'] = response.url
        item['sourceText'] = response.text
        item['followers'] = 0

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('script')[2].text.strip('window._sharedData = ').strip(';')
            data = json.loads(text)
            user = data['entry_data']['ProfilePage'][0]['user']
            try:
                item['sourceRef'] = user['id']
                if len(item['sourceRef']) < 1:
                    item['sourceRef'] = user['username']
            except:
                pass

            extract = lambda key:user.get(key,'') if user.get(key,'') is not None else ''
            item['name'] = extract('full_name')
            item['biography'] = extract('biography')
            item['external_url'] = extract('external_url')
            item['profile_pic_url'] = extract('profile_pic_url')

            try:
                item['followers'] = int(user['followed_by']['count'])
            except:
                item['followers'] = 0

            try:
                item['follows'] = user.get('follows', {}).get('count', '')
            except:
                pass

            try:
                item['num_posts'] = user.get('media', {}).get('count', '')
            except:
                pass

        except Exception as e:
            self.logger.error("Method: (parse) Error: %s" % (str(e)))


        yield item