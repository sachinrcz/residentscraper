import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ArtistscrapeItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os

class ResidentSpider(scrapy.Spider):

    name = "ArtistSpider"

    domain = "https://www.residentadvisor.net"
    custom_settings = {
        'HOST': 'localhost',
        'DATABASE':'WDJP',
        'SQLUSERNAME':'root',
    }

    def start_requests(self):
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host="localhost", port=3306, user="root", passwd=password, db="WDJP")
        cursor = db.cursor()
        cursor.execute('SELECT * FROM WDJP.dj_artist_website WHERE sourceID=14;')
        data = cursor.fetchall()
        # urls = [row[3]+'/dates' for row in data]
        #

        # for url in urls:
        #     request = scrapy.Request(url=url, callback=self.parse)
        #     yield request
        url = 'https://www.residentadvisor.net/dj/astrix/dates'
        request = scrapy.Request(url=url, callback=self.parse)
        yield request

    def parse(self, response):
        temp =  response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li/h1/text()')
        if len(temp)>0 and 'Upcoming' in temp[0].extract():
            upcomingselector = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li')[0]
            events = upcomingselector.xpath('.//a[contains(@itemprop, "url")]/@href').extract()
            print(events)
            for event in events:
                request = scrapy.Request(url= self.domain + event,callback=self.parse_event)
                yield request


    def parse_event(self,response):
        item = ArtistscrapeItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['eventURL'] = response.url
        item['eventName'] = response.css('div#sectionHead').xpath('.//h1/text()').extract()[0]
        details = response.css('aside#detail').xpath('.//li')
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]
            if 'Date' in header:
                if not (',' in list[1]):
                    item['eventStartDate'] = list[2]
                    item['eventEndDate'] = list[2]
                    item['eventTime'] = list[3]
                else:
                    item['eventStartDate'] = list[2]
                    item['eventEndDate'] = list[4]
                    item['eventTime'] = list[5]
                continue

            if 'Venue' in header:
                item['venueName'] = list[1].strip()
                item['venueAdd'] = list[2].strip()
                for i in range(3,len(list)):
                    item['venueAdd'] = item['venueAdd']+', '+list[i].replace('\xa0','').strip()
                continue

            if 'Cost' in header:
                item['cost'] = list[1].strip()
                continue

            if 'Promoter' in header:
                item['promoter'] = list[1].strip()
                promoterlinks = detail.xpath('.//a/@href').extract()
                if len(promoterlinks)>0:
                    item['promoterURL'] = self.domain+promoterlinks[0]
                continue

            if 'age' in header:
                item['age'] = list[1].strip()
                continue

        item['members'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip()
        temp = response.css('div.left').xpath('.//p')[0].xpath('.//text()').extract()
        item['lineup'] = (''.join(temp)).replace('\n','').strip()
        temp = response.css('div.left').xpath('.//p')[1].xpath('.//text()').extract()
        item['eventDetail'] = (''.join(temp)).replace('\n', '').strip()

        if len(item['promoterURL']) > 2:
            request = scrapy.Request(url=item['promoterURL'],callback=self.parse_promoter)
            request.meta['item'] = item
            yield request
        else:
            yield item


    def parse_promoter(self,response):
        # inspect_response(response,self)
        item = response.meta['item']
        details = response.css('aside#detail').xpath('.//li')
        try:
            item['promoterName'] = response.css('div#sectionHead').xpath('.//h1//text()').extract()[0]
        except:
            item['promoterName'] = response.css('div.position').xpath('.//h1//text()').extract()[0]
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]

            if 'Region' in header:
                item['promoterRegion'] = list[1].strip()
                continue

            if 'Address' in header:
                item['promoterAdd'] = list[1].strip()
                continue

            if 'Phone' in header:
                item['promoterPhone'] = list[1].strip()
                continue

            if 'internet' in header:
                links = detail.xpath('.//a')
                for link in links:
                    text = link.xpath('.//text()').extract()[0]
                    href = link.xpath('.//@href').extract()[0]
                    if 'Website' in text:
                        item['promoterWebsite'] = href
                        continue
                    if 'Email' in text:
                        href = href.replace('/cdn-cgi/l/email-protection#','').strip()
                        item['promoterEmail'] = href
                        continue
                    if 'Facebook' in text:
                        item['promoterFacebook'] = href
                        continue
                    if 'Instagram' in text:
                        item['promoterInstagram'] = href
                        continue
                    if 'Twitter' in text:
                        item['promoterTwitter'] = href
                        continue
                continue



        yield item

    def decodeEmail(self,code):
        email = ''
        c = int(code[:2], 16)
        for a in range(2, len(code), 2):
            l = int(code[a:a + 2], 16) ^ c
            email += chr(l)
        return email