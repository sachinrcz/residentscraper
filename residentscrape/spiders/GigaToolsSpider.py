import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ArtistItem
from residentscrape.items import ResidentItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os
import datetime
from scrapy.utils.project import get_project_settings
import logging
from urllib.parse import urljoin

class GigaToolsSpider(scrapy.Spider):

    name = "GigaToolsSpider"

    domain = "https://gigs.gigatools.com"

    logger = logging.getLogger("GigaToolsSpider")

    project_settings = get_project_settings()

    custom_settings = {
        "SOURCE_ID": project_settings['GIGATOOL_SOURCE_ID']
    }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        # password = os.environ.get('SECRET_KEY')
        # db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        # cursor = db.cursor()
        #
        # ## Get Artist URL from old database
        # cursor.execute("SELECT * FROM WDJP.dj_artist_website WHERE sourceID={} LIMIT 100;".format(self.custom_settings['GIGATOOL_SOURCE_ID']))
        # data = cursor.fetchall()
        # urls = [row[3].strip() for row in data]
        #
        # ## Get Artist URL from scrapeArtist table
        # # cursor.execute("SELECT * FROM WDJPNew.scrape_Artists where sourceID =1 order by refreshed LIMIT 1;")
        # # data = cursor.fetchall()
        # # urls = [row[18].strip() for row in data]
        #
        #
        # for url in urls:
        #     url = url.replace('http://','https://').strip()
        #     if 'https://gigs.gigatools.com/user' in url:
        #         request = scrapy.Request(url=url, callback=self.parse)
        #         yield request
        #     else:
        #         if len(url) > 6 and 'gigs.gigatools.com/user' in url:
        #             request = scrapy.Request(url='https://'+url, callback=self.parse)
        #             yield request

        ##For testing with single start url
        url = 'http://gigs.gigatools.com/user/LauraJones'
        request = scrapy.Request(url=url, callback=self.parse)
        yield request