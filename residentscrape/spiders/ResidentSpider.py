import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ArtistItem
from residentscrape.items import ResidentItem
import requests
import json
from bs4 import BeautifulSoup
import MySQLdb
import os
from scrapy.utils.project import get_project_settings
import logging
import sys
import datetime
import usaddress

class ResidentSpider(scrapy.Spider):

    name = "ResidentSpider"

    domain = "https://www.residentadvisor.net"

    logger = logging.getLogger("ResidentSpider")

    project_settings = get_project_settings()
    custom_settings = {
        "SOURCE_ID": project_settings['BIT_SOURCE_ID']
    }


    def start_requests(self):
        self.custom_settings = get_project_settings()
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'],
                             passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()

        ## Get Artist URL from old database
        cursor.execute("SELECT * FROM dj_artist_website WHERE sourceID=2 LIMIT 1000 ;")
        data = cursor.fetchall()
        urls = [row[3].strip() for row in data]

        ## Get Artist URL from scrapeArtist table
        # cursor.execute("SELECT * FROM WDJPNew.scrape_Artists where sourceID =2 order by refreshed LIMIT 1;")
        # data = cursor.fetchall()
        # urls = [row[18].strip() for row in data]

        ## Loop through every artist URL
        for url in urls:
            url = url.replace('http://', 'https://').strip()
            if 'https' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
            else:
                if len(url) > 6:
                    request = scrapy.Request(url='https://' + url, callback=self.parse)
                    yield request


        ##For testing with single start url
        # url = 'https://www.residentadvisor.net/dj/markknight'
        # request = scrapy.Request(url=url, callback=self.parse)
        # yield request


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
            item['name'] = response.css('div#sectionHead').xpath('.//h1/text()').extract()[0]
        except:
            item['name'] = response.css('div.position').xpath('.//h1/text()').extract()[0]
        item['sourceURL'] = response.url
        if self.custom_settings['SAVE_SOURCE'] == 1:
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
                try:
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
                except:
                    pass

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
        try:
            item['biography'] = response.xpath('//article')[0].extract()
        except Exception as e:
            self.logger.error("Method: (parse_biography) Error %s" % (e.args[0]))
        yield item

        request = scrapy.Request(url=item['sourceURL']+'/dates', callback=self.parse_events, dont_filter=True)
        request.meta['artistSourceRef'] = item['sourceRef']
        yield request




    def parse_events(self,response):

        mode = self.custom_settings['RA_MODE']
        year = int(self.custom_settings['RA_YEAR'])
        urls = []
        if 'upcoming' in mode:
            temp = response.css('ul.mobile-pr24-tablet-desktop-pr8')[0].xpath('.//li/h1/text()')
            if len(temp) > 0 and 'Upcoming' in temp[0].extract():
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
                try:
                    urls = urls[:(2017-year)]
                except:
                    pass
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
        item['eventGoing'] = 0
        item['venueFollowers'] = 0
        item['promoterFollowers'] = 0
        item['venueGeoLat'] = None
        item['venueGeoLong'] = None
        item['venueSourceRef'] = '-1'
        item['venueName'] = 'TBA/No Link'
        item['venueTBAInsert'] = True

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
        if self.custom_settings['SAVE_SOURCE'] == 1:
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
            venueURL = response.css('li.but.circle-left.bbox')[0].xpath('.//a/@href').extract()[0]
            if len(venueURL.split('/')) == 4:
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
        if self.custom_settings['SAVE_SOURCE'] == 1:
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


        # Venue City or Region
        try:
            temp = response.css('li.but.circle-left.bbox')[0].xpath('.//a//text()').extract()[0].strip()
            if 'north' in temp.lower() or 'east' in temp.lower() or 'south' in temp.lower() or 'west' in temp.lower() :
                item['venueRegion'] = temp
                item['venueCity'] = ''
            else:
                item['venueCity'] = temp
        except:
            pass



        ## Split Address
        try:
            if len(item['venueAddress']) > 1 and 'united state' in item['venueCountry'].lower():
                data = usaddress.tag(item['venueAddress'],tag_mapping={
                       'Recipient': 'recipient',
                       'AddressNumber': 'address1',
                       'AddressNumberPrefix': 'address1',
                       'AddressNumberSuffix': 'address1',
                       'StreetName': 'address1',
                       'StreetNamePreDirectional': 'address1',
                       'StreetNamePreModifier': 'address1',
                       'StreetNamePreType': 'address1',
                       'StreetNamePostDirectional': 'address1',
                       'StreetNamePostModifier': 'address1',
                       'StreetNamePostType': 'address1',
                       'CornerOf': 'address1',
                       'IntersectionSeparator': 'address1',
                       'LandmarkName': 'address1',
                       'USPSBoxGroupID': 'address1',
                       'USPSBoxGroupType': 'address1',
                       'USPSBoxID': 'address1',
                       'USPSBoxType': 'address1',
                       'BuildingName': 'address2',
                       'OccupancyType': 'address2',
                       'OccupancyIdentifier': 'address2',
                       'SubaddressIdentifier': 'address2',
                       'SubaddressType': 'address2',
                    })[0]
                item['venueState'] = data['StateName']
                item['venueZip'] = data['ZipCode']
                item['venueStreet'] = data['address1']
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
        if self.custom_settings['SAVE_SOURCE'] == 1:
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