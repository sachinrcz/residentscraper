import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ArtistItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os

class ArtistSpider(scrapy.Spider):

    name = "ArtistSpider"

    domain = "https://www.residentadvisor.net"
    custom_settings = {
        'HOST': 'localhost',
        'DATABASE':'WDJPNew',
        'SQLUSERNAME':'root',
        'sourceID': '2',
    }


    def start_requests(self):
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()
        cursor.execute("SELECT * FROM WDJP.dj_artist_website WHERE sourceID=2;")
        data = cursor.fetchall()
        urls = [row[3].strip() for row in data]
        for url in urls:
            url = url.replace('http://','https://').strip()
            if 'https' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
            else:
                if len(url) > 6:
                    request = scrapy.Request(url='https://'+url, callback=self.parse)
                    yield request
        ##For testing with single start url
        # url = 'https://www.residentadvisor.net/dj/basementjaxx'
        # request = scrapy.Request(url=url, callback=self.parse)
        # yield request

    def parse(self,response):
        item = ArtistItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['sourceRef'] = response.url.split('/')[-1].strip()
        if len(item['sourceRef']) < 1:
            item['sourceRef'] = response.url.split('/')[-2].strip()
        try:
            item['name'] = response.css('div#sectionHead').xpath('.//h1/text()').extract()[0]
        except:
            item['name'] = response.css('div.position').xpath('.//h1/text()').extract()[0]
        item['sourceURL'] = response.url
        item['sourceText'] = response.text
        details = response.css('aside#detail').xpath('.//li')
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]
            if 'Real name' in header:
                item['realName'] = list[1].strip()
                continue

            if 'Aliases' in header:
                item['aliases'] = list[1].strip()
                continue

            if 'Country' in header:
                item['country'] = list[-1].strip()
                continue

            if 'internet' in header:
                links = detail.xpath('.//a')
                for link in links:
                    text = link.xpath('.//text()').extract()[0]
                    href = link.xpath('.//@href').extract()[0]
                    if 'Website' in text:
                        item['website'] = href
                        continue
                    if 'Email' in text:
                        href = href.replace('/cdn-cgi/l/email-protection#','').strip()
                        item['email'] = self.decodeEmail(href)
                        continue
                    if 'Facebook' in text:
                        item['facebook'] = href
                        continue
                    if 'Instagram' in text:
                        item['instagram'] = href
                        continue
                    if 'Twitter' in text:
                        item['twitter'] = href
                        continue
                    if 'SoundCloud' in text:
                        item['soundcloud'] = href
                        continue
                    if 'Band' in text:
                        item['bandcamp'] = href
                        continue
                    if 'Discog' in text:
                        item['discog'] = href
                        continue

                continue
        try:
            item['followers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip().replace(',','').strip()
            item['followers'] = int(item['followers'])
        except:
            item['followers'] = 0

        request = scrapy.Request(url=self.domain+'/dj/'+item['sourceRef']+'/biography', callback=self.parse_biography)
        request.meta['item'] = item
        yield request


    def parse_biography(self,response):
        item = response.meta['item']
        item['biography'] = response.xpath('//article')[0].extract()
        yield item



    def decodeEmail(self,code):
        email = ''
        c = int(code[:2], 16)
        for a in range(2, len(code), 2):
            l = int(code[a:a + 2], 16) ^ c
            email += chr(l)
        return email