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
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()
        cursor.execute("SELECT * FROM WDJP.dj_artist_website WHERE sourceID=1 LIMIT 1000;")
        data = cursor.fetchall()
        urls = [row[3].strip() for row in data]
        for url in urls:
            url = url.replace('http://','https://').strip()
            if 'https' in url and 'bandsintown' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
            else:
                if len(url) > 6 and 'bandsintown' in url:
                    request = scrapy.Request(url='https://'+url, callback=self.parse)
                    yield request
        ##For testing with single start url
        # url = 'https://www.bandsintown.com/Bicep'
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

        try:
            names = response.css('section.content-container').xpath('.//a//text()').extract()
            links = response.css('section.content-container').xpath('.//a/@href').extract()
            links = [link.replace('/','').strip() for link in links]
            item['similarArtists'] = dict(zip(links,names))
        except:
            pass

        yield item

        mode = self.custom_settings['BIT_MODE']
        if 'upcoming' in mode:
            try:
                eventlinks = response.css('div.events-table').xpath('.//tr/@data-event-link').extract()
                for url in eventlinks:
                    request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
                    request.meta['artistSourceRef'] = item['sourceRef']
                    yield request
            except Exception as e:
                self.logger.error("Method: (parse) Error during extracting event links %s" % (e.args[0]))
        else:
            if 'history' in mode:
                url = response.css('div.tabs').xpath('.//a/@href').extract()[0]
                request = scrapy.Request(url=self.domain+url, callback=self.get_past_events, dont_filter=True)
                request.meta['artistSourceRef'] = item['sourceRef']
                yield request


    def get_past_events(self,response):
        try:
            eventlinks = response.css('div.events-table').xpath('.//tr/@data-event-link').extract()
            for url in eventlinks:
                request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
                request.meta['artistSourceRef'] = response.meta['artistSourceRef']
                yield request
        except Exception as e:
            self.logger.error("Method: (parse) Error during extracting event links %s" % (e.args[0]))

        try:
            nexturl = response.css('a.next_page').xpath('.//@href').extract()[0]
            request = scrapy.Request(url=self.domain+nexturl, callback=self.get_past_events, dont_filter=True)
            request.meta['artistSourceRef'] = response.meta['artistSourceRef']
            yield request
        except:
            pass

    def parse_event(self,response):
        item = ResidentItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['venueCapacity'] = 0
        item['eventFollowers'] = 0
        item['venueFollowers'] = 0
        item['promoterFollowers'] = 0
        item['artistSourceRef'] = response.meta['artistSourceRef']
        item['eventSourceURL'] = response.url
        item['eventSourceText'] = response.text
        try:
            item['eventSourceRef'] = response.url.split('/')[4].split('-')[0]
        except:
            pass

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

        ## Event Followers/ RSVP
        try:
            item['eventFollowers'] = int(response.css('div.rsvp-count').css('span.count::text').extract()[0].strip())
        except:
            pass

        ## Event LineUp
        try:
            item['eventLineup'] = ''.join(response.css('strong.lineup-performers').xpath('.//span//text()').extract())
        except:
            pass

        ## Event Description
        try:
            item['eventDescription'] = response.css('div.event-description::text').extract()[0].strip()
        except:
            pass


        ## Venue Information
        try:
            item['venueName'] = response.css('h2.venue-name').xpath('.//a/text()').extract()[0].strip()
        except Exception as e:
            pass

        try:
            item['venueSourceURL'] = response.css('h2.venue-name').xpath('.//a/@href').extract()[0].strip()
            item['venueSourceRef'] = item['venueSourceURL'].split('/')[4].split('-')[0]
        except:
            pass


        ## Address information
        try:
            item['venueAddress'] = response.xpath('//h3[contains(@itemprop,"streetAddress")]//text()').extract()[0].strip()
        except:
            pass
        try:
            item['venueAddress'] = response.xpath('//h3[contains(@itemprop,"streetAddress")]//text()').extract()[0].strip()
        except:
            pass

        try:
            locationURL = self.domain+response.css('h2.venue-location').xpath('.//a/@href').extract()[0].strip()
            item['venueCity'] = response.xpath('//span[contains(@itemprop,"addressLocality")]//text()').extract()[0].strip()
        except:
            pass

        try:
            item['venueCountry'] = response.xpath('//span[contains(@itemprop,"addressCountry")]//text()').extract()[0].strip()
        except:
            pass

        if len(item['venueSourceURL']) > 0 :
            request = scrapy.Request(url=item['venueSourceURL'],callback=self.parse_venue, dont_filter=True)
            request.meta['item'] = item
            yield request
        else:
            yield item


    def parse_venue(self,response):
        item = response.meta['item']
        item['venueSourceText'] = response.text

        try:
            item['venuePhone'] = response.css('div.venue-phone::text').extract()[0]
        except:
            pass

        try:
            item['venueGoogleMap'] = response.css('div.sidebar-image').xpath('.//a/@href').extract()[0]
            cords =  item['venueGoogleMap'].split('ll=')[1].split('&')[0].split('%2C')
            item['venueGeoLat'] = cords[0]
            item['venueGeoLong'] = cords[1]

        except:
            pass

        yield item
