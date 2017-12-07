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

class BandsInTownSpider(scrapy.Spider):

    name = "BandsInTownSpider"

    domain = "https://www.bandsintown.com"

    logger = logging.getLogger("BITArtistSpider")

    custom_settings = {
        'SOURCE_ID': '1',
    }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        # password = os.environ.get('SECRET_KEY')
        # db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        # cursor = db.cursor()
        # cursor.execute("SELECT * FROM WDJP.dj_artist_website WHERE sourceID=1 LIMIT 10 ;")
        # data = cursor.fetchall()
        # urls = [row[3].strip() for row in data]
        # for url in urls:
        #     url = url.replace('http://','https://').strip()
        #     if 'https' in url and 'bandsintown' in url:
        #         request = scrapy.Request(url=url, callback=self.parse)
        #         yield request
        #     else:
        #         if len(url) > 6 and 'bandsintown' in url:
        #             request = scrapy.Request(url='https://'+url, callback=self.parse)
        #             yield request
        ##For testing with single start url
        url = 'https://www.bandsintown.com/Bicep'
        request = scrapy.Request(url=url, callback=self.parse)
        yield request

    def parse(self,response):
        item = ArtistItem()
        for field in item.fields:
            item.setdefault(field, '')
        try:
            item['sourceRef'] = response.url.split('/')[-1].strip()
            if len(item['sourceRef']) < 1:
                item['sourceRef'] = response.url.split('/')[-2].strip()
        except:
            pass
        try:
            item['name'] = response.css('header.headline').xpath('.//h1/div//text()').extract()[0]
        except:
            pass
        item['sourceURL'] = response.url
        item['sourceText'] = response.text

        try:
            item['followers'] = response.css('p.count::text').extract()[0].replace('Trackers','').replace(',','').strip()
            item['followers'] = int(item['followers'])
        except:
            item['followers'] = 0

        yield item

        try:
            eventlinks = response.css('div.events-table').xpath('.//tr/@data-event-link').extract()
            for url in eventlinks:
                request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
                yield request
        except Exception as e:
            self.logger.error("Method: (parse) Error during extracting event links %s" % (e.args[0]))



    def parse_event(self,response):
        item = ResidentItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['eventSourceURL'] = response.url
        item['eventSourceText'] = response.text

        ## Extract Date Time
        try:
            list = response.css('h3.event-date')[0].xpath('.//text()').extract()
            text = ''.join([item.strip() for item in list])
            item['eventStartDate'] = response.xpath('//meta[contains(@itemprop,"startDate")]/@content').extract()[0].strip()
            item['eventStartTime'] = text.split('-')[1].strip()
            item['eventEndDate'] = response.xpath('//meta[contains(@itemprop,"endDate")]/@content').extract()[
                0].strip()
            item['eventEndTime'] = text.split('-')[2].strip()
        except:
            pass

        ## Convert Date
        try:
            item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].strip(), '%Y-%m-%d').date()
        except:
            item['startDate'] = None
        try:
            item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%Y-%m-%d').date()
        except:
            item['endDate'] = None

        try:
            item['biography'] = response.xpath('//article')[0].extract()
        except Exception as e:
            self.logger.error("Method: (parse_biography) Error %s" % (e.args[0]))
        yield item



