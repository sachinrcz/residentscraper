import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ResidentItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os
import sys

class EventSpider(scrapy.Spider):

    name = "EventSpider"

    domain = "https://www.residentadvisor.net"
    custom_settings = {
        'HOST': 'localhost',
        'DATABASE':'WDJPNew',
        'SQLUSERNAME':'root',
        'sourceID':'2',
    }

    def start_requests(self):
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()
        cursor.execute('SELECT sourceURL FROM WDJPNew.scrape_Artists LIMIT 100;')
        data = cursor.fetchall()
        urls = [row[0]+'/dates' for row in data]
        for url in urls:
            if 'http' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
        ##For testing with single start url
        # url = 'https://www.residentadvisor.net/dj/astrix/dates'
        # request = scrapy.Request(url=url, callback=self.parse)
        # yield request

    def parse(self, response):
        temp =  response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li/h1/text()')
        if len(temp)>0 and 'Upcoming' in temp[0].extract():
            upcomingselector = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li')[0]
            events = upcomingselector.xpath('.//a[contains(@itemprop, "url")]/@href').extract()
            for event in events:
                request = scrapy.Request(url= self.domain + event,callback=self.parse_event)
                yield request


    def parse_event(self,response):
        item = ResidentItem()
        # Initialize
        for field in item.fields:
            item.setdefault(field, '')
        item['eventSourceRef'] = 0
        item['venueSourceRef'] = 0
        item['promoterSourceRef'] = 0
        item['venueCapacity'] = 0
        item['eventFollowers'] = 0
        item['venueFollowers'] = 0
        item['promoterFollowers'] = 0

        item['eventSourceURL'] = response.url
        try:
            item['eventName'] = response.css('div#sectionHead').xpath('.//h1//text()').extract()[0]
        except:
            item['eventName'] = response.css('div.position').xpath('.//h1//text()').extract()[0]
        try:
            item['eventSourceRef'] = int(response.url.split('/')[-1].strip())
        except:
            pass
        try:
            if len(item['eventSourceRef']) < 1:
                item['eventSourceRef'] = int(response.url.split('/')[-2].strip())
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
                if len(item['eventVenueURL']) == 0 :
                    item['eventTBA'] = ', '.join(list[1:])
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
                item['eventTicketPrice'] = list[1].strip()
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
                item['eventMinAge'] = list[1].strip()
                continue

        # Members Count
        try:
            item['eventFollowers'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip().replace(',','').strip()
            item['eventFollowers'] = int(item['eventFollowers'])
        except:
            item['eventFollowers'] = 0

        # Event Lineup and description
        try:
            temp = response.css('div.left').xpath('.//p')[0].xpath('.//text()').extract()
            item['eventLineup'] = (''.join(temp)).replace('\n','').strip()
            temp = response.css('div.left').xpath('.//p')[1].xpath('.//text()').extract()
            item['eventDescription'] = (''.join(temp)).replace('\n', '').strip()
        except:
            pass

        # EventAdmin Section
        temp = response.css('div#event-item').css('div.links').xpath('.//li/a')
        try:
            for i in temp:
                test = i.xpath('.//@href').extract()[0]
                if 'profile' in test:
                    item['eventAdmin'] = i.xpath('.//text()').extract()[0].strip()
                    break
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
            item['venueSourceRef'] = int(item['eventVenueURL'].split('id=')[-1].strip())
        except:
            pass

        if len(item['eventVenueURL']) > 0:
            request = scrapy.Request(url=item['eventVenueURL'],callback=self.parse_venue,dont_filter=True)
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
            item['venueSourceRef'] = int(response.url.split('id=')[-1].strip())
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
            item['promoterSourceRef'] = int(response.url.split('id=')[-1].strip())
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
                item['promoterPhone'] = list[1].strip()
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