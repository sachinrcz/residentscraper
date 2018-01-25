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

    logger = logging.getLogger("BandsInTownSpider")

    project_settings = get_project_settings()
    custom_settings = {
        "SOURCE_ID": project_settings['BIT_SOURCE_ID']
    }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()

        ## Get Artist URL from old database
        cursor.execute("SELECT * FROM dj_artist_website WHERE sourceID=1;")
        data = cursor.fetchall()
        urls = [row[3].strip() for row in data]

        ## Get Artist URL from scrapeArtist table
        # cursor.execute("SELECT * FROM WDJPNew.scrape_Artists where sourceID =1 order by refreshed LIMIT 1;")
        # data = cursor.fetchall()
        # urls = [row[18].strip() for row in data]


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
            item['name'] = response.css('div.artistInfoContainer-0a02819b::text').extract()[0].strip()
        except:
            pass
        item['sourceURL'] = response.url
        if self.custom_settings['SAVE_SOURCE'] == 1:
            item['sourceText'] = response.text

        try:
            item['followers'] = response.css('span.artistInfoContainer-452c7757::text').extract()[0].replace('Trackers','').replace(',','').strip()
            item['followers'] = int(item['followers'])
        except:
            item['followers'] = 0

        try:
            names = response.css('div.artist-b4271661').xpath('.//a//text()').extract()
            links = response.css('div.artist-b4271661').xpath('.//a/@href').extract()
            # links = [link.replace('/','').strip() for link in links]
            item['similarArtists'] = dict(zip(links,names))
        except:
            pass


        try:
            item['profile_pic_url'] = response.css('div.artistInfoContainer-2b4bfca0').xpath('.//img/@src').extract()[0]
        except:
            pass

        yield item

        artistName = item['name']
        sourceRef = item['sourceRef']
        item = None

        try:
            scriptText = response.css('script::text').extract()[5]
            events = json.loads(scriptText)
            for event in events:
                item = ResidentItem()
                for field in item.fields:
                    item.setdefault(field, '')
                item['venueCapacity'] = 0
                item['eventFollowers'] = 0
                item['eventGoing'] = 0
                item['venueFollowers'] = 0
                item['promoterFollowers'] = 0
                item['venueGeoLat'] = None
                item['venueGeoLong'] = None
                item['venueTBAInsert'] = True

                item['artistSourceRef'] = sourceRef
                item['eventName'] = event.get('name','')
                item['eventSourceURL'] = event.get('url','')
                item['eventStartDate'] = event.get('startDate','')
                item['eventEndDate'] = event.get('endDate','')
                item['eventDescription'] = event.get('description','')
                item['event_image_url'] = event.get('image','')
                try:
                    location = event['location']
                    item['venueAddress'] = location.get('address','')
                    geo = location['geo']
                    item['venueGeoLong'] = geo['longitude']
                    item['venueGeoLat'] = geo['latitude']
                    item['venueName'] = location['name']
                except:
                    pass
                request = scrapy.Request(url=item['eventSourceURL'], callback=self.parse_event, dont_filter=True)
                request.meta['item'] = item
                yield request

        except:
            self.logger.log('No events found')
            pass




        ### Code for old UI
        # mode = self.custom_settings['BIT_MODE']
        # if 'upcoming' in mode:
        #     try:
        #         eventlinks = response.css('div.events-table').xpath('.//tr/@data-event-link').extract()
        #         for url in eventlinks:
        #             request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
        #             request.meta['artistSourceRef'] = item['sourceRef']
        #             request.meta['artistName'] = item['name']
        #             yield request
        #     except Exception as e:
        #         self.logger.error("Method: (parse) Error during extracting event links %s" % (e.args[0]))
        # else:
        #     if 'history' in mode:
        #         url = response.css('div.tabs').xpath('.//a/@href').extract()[0]
        #         request = scrapy.Request(url=self.domain+url, callback=self.get_past_events, dont_filter=True)
        #         request.meta['artistSourceRef'] = item['sourceRef']
        #         request.meta['artistName'] = item['name']
        #         yield request


    # def get_past_events(self,response):
    #     try:
    #         eventlinks = response.css('div.events-table').xpath('.//tr/@data-event-link').extract()
    #         for url in eventlinks:
    #             request = scrapy.Request(url=url, callback=self.parse_event, dont_filter=True)
    #             request.meta['artistSourceRef'] = response.meta['artistSourceRef']
    #             request.meta['artistName'] = response.meta['artistName']
    #             yield request
    #     except Exception as e:
    #         self.logger.error("Method: (parse) Error during extracting event links %s" % (e.args[0]))
    #
    #     try:
    #         nexturl = response.css('a.next_page').xpath('.//@href').extract()[0]
    #         request = scrapy.Request(url=self.domain+nexturl, callback=self.get_past_events, dont_filter=True)
    #         request.meta['artistSourceRef'] = response.meta['artistSourceRef']
    #         request.meta['artistName'] = response.meta['artistName']
    #         yield request
    #     except:
    #         pass

    def parse_event(self,response):
        item = response.meta['item']
        if self.custom_settings['SAVE_SOURCE'] == 1:
            item['eventSourceText'] = response.text

        try:
            item['eventSourceRef'] = response.url.split('/')[4].split('-')[0]
        except:
            pass



        ## Extract Time
        try:
            item['eventStartTime'] = response.css('div.eventInfoContainer-961898de::text').extract()[0]
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
            item['eventFollowers'] = int(response.css('div.rsvpsContainer-2f8a7ebf::text').extract()[0].strip('RSVPs').strip('RSVP'))
        except Exception as e:
            pass

        ## Event LineUp
        try:
            item['eventLineup'] = ''.join(response.css('div.lineupItem-d3a20891').xpath('.//a/text()').extract())
        except:
            pass

        try:
            item['venueSourceURL'] = response.css('div.eventInfoContainer-fda09035').xpath('.//a/@href').extract()[0].strip()
            item['venueSourceRef'] = item['venueSourceURL'].split('/')[4].split('-')[0]
        except:
            pass

        yield item

        # if len(item['venueSourceURL']) > 0 :
        #     request = scrapy.Request(url=item['venueSourceURL'],callback=self.parse_venue, dont_filter=True)
        #     request.meta['item'] = item
        #     yield request
        # else:
        #     yield item


    def parse_venue(self,response):
        item = response.meta['item']
        if self.custom_settings['SAVE_SOURCE'] == 1:
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
