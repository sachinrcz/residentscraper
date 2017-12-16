import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ResidentItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os
import sys
import datetime
import logging
from scrapy.utils.project import get_project_settings

class ResidentEventSpider(scrapy.Spider):

    name = "ResidentEventSpider"

    domain = "https://www.residentadvisor.net"

    custom_settings = {
        'SOURCE_ID': '2',
    }

    logger = logging.getLogger("ResidentEventSpider")

    def start_requests(self):
        self.custom_settings = get_project_settings()
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()
        cursor.execute('SELECT sourceArtistRef, sourceURL FROM WDJPNew.scrape_Artists WHERE sourceID={} LIMIT 1000;'.format('2'))
        rows = cursor.fetchall()
        # urls = [row[0]+'/dates' for row in data]
        for row in rows:
            url = row[1]+'/dates'
            artist = row[0]
            if 'http' in url:
                request = scrapy.Request(url=url, callback=self.parse, dont_filter=True)
                request.meta['artistSourceRef'] = artist
                yield request
        ##For testing with single start url
        # url = 'https://www.residentadvisor.net/dj/astrix/dates'
        # request = scrapy.Request(url=url, callback=self.parse)
        # request.meta['artistSourceRef'] = 'astrix'
        # yield request

    def parse(self, response):
        mode = self.custom_settings['RA_MODE']
        urls = []
        if 'upcoming' in mode:
            temp = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li/h1/text()')
            if len(temp)>0 and 'Upcoming' in temp[0].extract():
                upcomingselector = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li')[0]
                events = upcomingselector.xpath('.//a[contains(@itemprop, "url")]/@href').extract()
                urls = [self.domain + event for event in events]
                for url in urls:
                    request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
                    request.meta['artistSourceRef'] = response.meta['artistSourceRef']
                    yield request
        else:
            if 'history' in mode:
                temp = response.css('div.fl')[0].xpath('.//li/a/@href').extract()
                urls = [self.domain + x for x in temp]
                for url in urls:
                    request = scrapy.Request(url=url, callback=self.extract_hist_events, dont_filter=True)
                    request.meta['artistSourceRef'] = response.meta['artistSourceRef']
                    yield request


    def extract_hist_events(self, response):
        upcomingselector = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li')[0]
        events = upcomingselector.xpath('.//a[contains(@itemprop, "url summary")]/@href').extract()
        for event in events:
            request = scrapy.Request(url=self.domain + event, callback=self.parse_event, dont_filter=True)
            request.meta['artistSourceRef'] = response.meta['artistSourceRef']
            yield request


    def parse_event(self,response):
        item = ResidentItem()
        # Initialize
        for field in item.fields:
            item.setdefault(field, '')
        item['artistSourceRef'] = response.meta['artistSourceRef']
        item['venueCapacity'] = 0
        item['eventFollowers'] = 0
        item['venueFollowers'] = 0
        item['promoterFollowers'] = 0
        item['scrapeVenueID'] = -1
        item['venueGeoLat'] = None
        item['venueGeoLong'] = None
        item['venueSourceRef'] = '-1'
        item['venueName'] = 'TBA/No Link'

        item['eventSourceURL'] = response.url
        try:
            item['eventName'] = response.css('div#sectionHead').xpath('.//h1//text()').extract()[0]
        except:
            item['eventName'] = response.css('div.position').xpath('.//h1//text()').extract()[0]
        try:
            item['eventSourceRef'] = response.url.split('/')[-1].strip()
        except:
            pass
        try:
            if len(item['eventSourceRef']) < 1:
                item['eventSourceRef'] = response.url.split('/')[-2].strip()
        except:
            pass
        item['eventSourceText'] = response.text
        details = response.css('aside#detail').xpath('.//li')
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]
            if 'Date' in header:
                try:
                    if not (',' in list[1]):
                        item['eventStartDate'] = list[2]
                        item['eventStartTime'] = list[3]
                    else:
                        item['eventStartDate'] = list[2]
                        item['eventEndDate'] = list[4]
                        item['eventStartTime'] = list[5]
                except:
                    pass
                try:
                    temp = item['eventStartTime']
                    if '-' in temp:
                        temp = temp.split('-')
                        item['eventStartTime'] = temp[0].strip()
                        item['eventEndTime'] = temp[1].strip()
                except:
                    pass
                continue

            if 'Venue' in header:
                try:
                    links = detail.xpath('.//a')
                    for link in links:
                        href = link.xpath('.//@href').extract()[0]
                        if 'club' in href:
                            item['eventVenueURL'] = self.domain + href
                except:
                    pass
                try:
                    if len(item['eventVenueURL']) == 0 :
                        item['eventVenueAddress'] = ', '.join(list[1:])
                        item['venueAddress'] = ', '.join(list[1:])
                    temp = detail.xpath('.//a')[0].xpath('.//@href').extract()[0]
                    if 'local' in temp:
                        item['venueCountry'] = detail.xpath('.//a')[0].xpath('.//text()').extract()[0].replace('\xa0',
                                                                                                     '').strip()
                except:
                    pass
                continue

            if 'internet' in header:
                try:
                    links = detail.xpath('.//a')
                    for link in links:
                        text = link.xpath('.//text()').extract()[0]
                        href = link.xpath('.//@href').extract()[0]
                        if 'Facebook' in text:
                            item['eventFacebook'] = href
                            continue
                        if 'Twitter' in text:
                            item['venueTwitter'] = href
                            continue
                except:
                    pass
                continue

            if 'Cost' in header:
                try:
                    item['eventTicketPrice'] = list[1].strip()
                except:
                    pass
                continue

            if 'Promoter' in header:
                try:
                    item['eventPromoters'] = ','.join(detail.xpath('.//a//text()').extract())
                    promoterlinks = detail.xpath('.//a/@href').extract()
                    item['eventPromotersURL'] = []
                    for link in promoterlinks:
                        item['eventPromotersURL'].append(self.domain+link)
                except:
                    pass
                continue

            if 'age' in header:
                try:
                    item['eventMinAge'] = list[1].strip()
                except:
                    pass
                continue

        # Members Count
        try:
            item['eventFollowers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip().replace(',','').strip()
            item['eventFollowers'] = int(item['eventFollowers'])
        except:
            item['eventFollowers'] = 0

        ## Convert Date
        try:
            item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].strip(), '%d %b %Y').date()
        except:
            item['startDate'] = None
        try:
            item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%d %b %Y').date()
        except:
            item['endDate'] = None

        # Event Lineup and description
        try:
            temp = response.css('div.left').xpath('.//p')[0].xpath('.//text()').extract()
            item['eventLineup'] = (''.join(temp)).replace('\n','').strip()
            temp = response.css('div.left').xpath('.//p')[1].xpath('.//text()').extract()
            item['eventDescription'] = (''.join(temp)).replace('\n', '').strip()
        except:
            pass

        # EventAdmin Section
        temp = response.css('div#event-item').css('div.links').xpath('.//ul')
        try:
            for i in temp:
                head = i.xpath('.//text()').extract()[0]
                if 'admin' in head:
                    item['eventAdmin'] = i.xpath('.//li/a/text()').extract()[0].strip()
                    continue
                if 'Promotional' in head:
                    links = i.xpath('.//li/a')
                    for link in links:
                        text = link.xpath('.//text()').extract()[0]
                        if 'Facebook' in text:
                            item['eventFacebook'] = link.xpath('.//@href').extract()[0]
                            continue
                        item['eventPromotional'] =  link.xpath('.//@href').extract()[0]
        except:
            pass

        # Ticket Info Section
        try:
            temp = ', '.join(response.css('li#tickets')[0].xpath('.//li//text()').extract())
            item['eventTicketInfo'] = temp
            temp = response.css('li#tickets')[0].css('li.onsale.but').xpath('.//text()').extract()
            item['eventTicketPrice'] = temp[0].strip()
            item['eventTicketTier'] = temp[1].strip()
        except:
            pass


        try:
            item['venueCity'] = response.css('li.but.circle-left.bbox')[0].xpath('.//a//text()').extract()[0]
        except:
            pass

        if len(item['eventVenueURL']) > 0:
            try:
                item['venueSourceRef'] = item['eventVenueURL'].split('id=')[-1].strip()
            except:
                pass
            request = scrapy.Request(url=item['eventVenueURL'],callback=self.parse_venue, dont_filter=True)
            request.meta['item'] = item
            yield request
        else:
            if len(item['eventPromotersURL']) > 0:
                for url in item['eventPromotersURL']:
                    request = scrapy.Request(url=url, callback=self.parse_promoter, dont_filter=True)
                    request.meta['item'] = item
                    yield request
            else:
                yield item


    def parse_venue(self,response):
        item = response.meta['item']
        details = response.css('aside#detail').xpath('.//li')
        item['venueSourceURL'] = response.url
        item['venueSourceText'] = response.text
        try:
            item['venueSourceRef'] = response.url.split('id=')[-1].strip()
        except:
            pass
        try:
            item['venueName'] = response.css('div#sectionHead').xpath('.//h1//text()').extract()[0]
        except:
            item['venueName'] = response.css('div.position').xpath('.//h1//text()').extract()[0]
        try:
            item['venueFollowers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip().replace(',','').strip()
            item['venueFollowers'] = int(item['venueFollowers'])
        except:
            item['venueFollowers'] = 0

        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]

            if 'Address' in header:
                try:
                    item['venueAddress'] = ', '.join(list[1:-1])
                    temp = detail.xpath('.//a')[0].xpath('.//@href').extract()[0]
                    if 'local' in temp:
                        item['venueCountry'] = detail.xpath('.//a')[0].xpath('.//text()').extract()[0].replace('\xa0','').strip()
                        continue
                except:
                    pass

                continue

            if 'Aka' in header:
                item['venueAka'] = ', '.join(list[1:])
                continue

            if 'Capacity' in header:
                try:
                    item['venueCapacity'] = int(list[1].replace(',','').strip())
                except:
                    pass
                continue

            if 'Phone' in header:
                item['venuePhone'] = ', '.join(list[1:])
                continue

            if 'internet' in header:
                try:
                    links = detail.xpath('.//a')
                    for link in links:
                        text = link.xpath('.//text()').extract()[0]
                        href = link.xpath('.//@href').extract()[0]
                        if 'Website' in text:
                            item['venueWebsite'] = href
                            continue
                        if 'Email' in text:
                            href = href.replace('/cdn-cgi/l/email-protection#','').strip()
                            item['venueEmail'] = self.decodeEmail(href)
                            continue
                        if 'Facebook' in text:
                            item['venueFacebook'] = href
                            continue
                        if 'Google' in text:
                            item['venueGoogleMap'] = href
                            continue
                        if 'Instagram' in text:
                            item['venueInstagram'] = href
                            continue
                        if 'Twitter' in text:
                            item['venueTwitter'] = href
                except:
                    pass
                continue

        # Members/Followers Count
        try:
            item['venueFollowers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[
                0].replace('\n', '').strip().replace(',', '').strip()
            item['venueFollowers'] = int(item['eventFollowers'])
        except:
            item['venueFollowers'] = 0

        # Venue Description
        try:
            item['venueDescription'] = response.css('div.pr24.pt8').xpath('.//text()').extract()[0]
        except:
            pass


        # Venue City
        try:
            item['venueCity'] = response.css('li.but.circle-left.bbox')[0].xpath('.//a//text()').extract()[0]
        except:
            pass

        if len(item['eventPromotersURL']) > 0:
            for url in item['eventPromotersURL']:
                request = scrapy.Request(url=url,callback=self.parse_promoter,dont_filter=True)
                request.meta['item'] = item
                yield request
        else:
            yield item




    def parse_promoter(self,response):
        # inspect_response(response,self)
        item = response.meta['item']
        item['promoterSourceURL'] = response.url
        item['promoterSourceText'] = response.text
        try:
            item['promoterSourceRef'] = response.url.split('id=')[-1].strip()
        except:
            pass
        details = response.css('aside#detail').xpath('.//li')
        try:
            item['promoterName'] = response.css('div#sectionHead').xpath('.//h1//text()').extract()[0]
        except:
            item['promoterName'] = response.css('div.position').xpath('.//h1//text()').extract()[0]
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]

            if 'Region' in header:
                try:
                    item['promoterRegion'] = detail.xpath('.//a')[0].xpath('.//text()').extract()[0].replace('\xa0','').strip()
                except:
                    pass
                continue

            if 'Address' in header:
                item['promoterAddress'] = ', '.join(list[1:])
                continue

            if 'Phone' in header:
                try:
                    item['promoterPhone'] = list[1].strip()
                except:
                    pass
                continue

            if 'internet' in header:
                try:
                    links = detail.xpath('.//a')
                    for link in links:
                        text = link.xpath('.//text()').extract()[0]
                        href = link.xpath('.//@href').extract()[0]
                        if 'Website' in text:
                            item['promoterWebsite'] = href
                            continue
                        if 'Email' in text:
                            href = href.replace('/cdn-cgi/l/email-protection#','').strip()
                            item['promoterEmail'] = self.decodeEmail(href)
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
                        if 'Google' in text:
                            item['promoterGoogleMap'] = href
                            continue
                except:
                    pass
                continue


        # Members/Followers Count
        try:
            item['promoterFollowers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[
                0].replace('\n', '').strip().replace(',', '').strip()
            item['promoterFollowers'] = int(item['eventFollowers'])
        except:
            item['promoterFollowers'] = 0

        # Promoter Description
        try:
            item['promoterDescription'] = response.css('div.pr24.pt8').xpath('.//text()').extract()[0]
        except:
            pass

        yield item

    def decodeEmail(self,code):
        email = ''
        c = int(code[:2], 16)
        for a in range(2, len(code), 2):
            l = int(code[a:a + 2], 16) ^ c
            email += chr(l)
        return email