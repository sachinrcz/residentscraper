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



class SongkickSpider(scrapy.Spider):

    name = "SongkickSpider"

    domain = "https://www.songkick.com"

    logger = logging.getLogger("SongkickSpider")

    project_settings = get_project_settings()
    custom_settings = {
        "SOURCE_ID": project_settings['SONGKICK_SOURCE_ID']
    }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'], passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()

        ## Get Artist URL from old database
        cursor.execute("SELECT * FROM dj_artist_website WHERE sourceID={} LIMIT 1000;".format(self.custom_settings['SONGKICK_SOURCE_ID']))
        data = cursor.fetchall()
        urls = [row[3].strip() for row in data]

        ## Get Artist URL from scrapeArtist table
        # cursor.execute("SELECT * FROM WDJPNew.scrape_Artists where sourceID =1 order by refreshed LIMIT 1;")
        # data = cursor.fetchall()
        # urls = [row[18].strip() for row in data]


        for url in urls:
            url = url.replace('http://','https://').strip()
            if 'https' in url and 'songkick' in url:
                request = scrapy.Request(url=url, callback=self.parse)
                yield request
            else:
                if len(url) > 6 and 'songkick' in url:
                    request = scrapy.Request(url='https://'+url, callback=self.parse)
                    yield request

        ##For testing with single start url
        # url = 'https://www.songkick.com/artists/416962-armin-van-buuren'
        # request = scrapy.Request(url=url, callback=self.parse)
        # yield request


    def parse(self,response):
        item = ArtistItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['followers'] = 0

        try:
            item['sourceRef'] = response.url.split('/')[-1].split('-')[0].strip()
            if len(item['sourceRef']) < 1:
                item['sourceRef'] = response.url.split('/')[-1].strip()
        except:
            pass

        try:
            item['name'] = response.css('h1.h0::text').extract()[0].strip()
        except:
            pass
        item['sourceURL'] = response.url
        item['sourceText'] = response.text

        try:
            item['followers'] = response.css('li.popularity-highlight').xpath('.//strong//text()').extract()[0][1:-1]
            item['followers'] = int(item['followers'])
        except:
            pass


        try:
            item['biography'] = response.xpath('//div[@id="biography"]').extract()[0]
        except:
            pass


        try:
            names = response.css('div.component.related-artists').css('span.artist-name::text').extract()
            links = response.css('div.component.related-artists').xpath('.//li/a/@href').extract()
            links = [link.split('/')[-1] for link in links]
            item['similarArtists'] = dict(zip(links,names))
        except:
            pass


        yield item

        request = scrapy.Request(url=urljoin(response.url+'/','calendar'), callback=self.get_all_events)
        request.meta['artistSourceRef'] = item['sourceRef']
        request.meta['artistName'] = item['name']
        yield  request



    def get_all_events(self,response):

        urls = response.css('p.artists.summary').xpath('.//a/@href').extract()

        for url in urls:
            request = scrapy.Request(url=urljoin(self.domain, url), callback=self.parse_events)
            request.meta['artistSourceRef'] = response.meta['artistSourceRef']
            request.meta['artistName'] = response.meta['artistName']
            yield request

    def parse_events(self,response):
        item = ResidentItem()
        for field in item.fields:
            item.setdefault(field, '')

        ## Initialize Default Event Items
        item['eventFollowers'] = -1
        item['eventGoing'] = -1
        # item['promoterFollowers'] = 0
        item['artistSourceRef'] = response.meta['artistSourceRef']
        item['eventSourceURL'] = response.url
        item['eventSourceText'] = response.text

        ## Initialize Default Venue Items
        item['venueSourceRef'] = '-1'
        item['venueName'] = 'TBA/No Link'
        item['venueCapacity'] = -1
        item['venueFollowers'] = -1
        item['venueGeoLat'] = None
        item['venueGeoLong'] = None
        item['venueTBAInsert'] = False

        try:
            item['eventSourceRef'] = response.url.split('/')[4].split('-')[0]
        except:
            pass


        ## Extract Date Time
        try:
            text = response.css('div.date-and-name').xpath('.//p/text()').extract()[0]
            item['eventStartDate'] = text.split('–')[0].strip()
            item['eventEndDate'] = text.split('–')[1].strip()
        except:
            pass

        ## Convert Date
        try:
            item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].strip(), '%A %d %B %Y').date()
        except:
            item['startDate'] = None
        try:
            item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%A %d %B %Y').date()
        except:
            item['endDate'] = None

        ## Name Event
        try:
            if len(response.css('h1.h0').xpath('.//span/a/text()')) > 1:
                names = response.css('h1.h0').xpath('.//span/a/text()').extract()
                item['eventName'] = ' and '.join(names)
            else:
                item['eventName'] = response.css('h1.h0').xpath('.//span/a/text()').extract()[0].strip()
        except:
            try:
                item['eventName'] = response.css('h1.h0').xpath('.//span/text()').extract()[0].strip()
            except:
                pass



        ## Event Ticket URL
        try:
            text = response.css('div.ticket-wrapper').xpath('.//a/@href').extract()[0]
            item['eventTicketURL'] = urljoin(self.domain, text)
        except:
            pass

        ## Event Followers
        try:
            item['eventFollowers'] = response.css('div.component.attendance-listing.tracking').xpath('.//h5/text()').extract()[0].split(' ')[0].strip()
            item['eventFollowers'] = int(item['eventFollowers'])
        except:
            pass

        ## Event Going
        try:
            item['eventGoing'] = response.css('div.component.attendance-listing.im-going').xpath('.//h5/text()').extract()[0].split(' ')[0].strip()
            item['eventGoing'] = int(item['eventGoing'])
        except:
            pass

        ## Event LineUp
        try:
            linuplist = response.css('ul.festival').xpath('.//text()').extract()
            item['eventLineup'] = ', '.join([x for x in linuplist if x.strip() != ''])
        except:
            pass


        ## Venue URL
        try:
            item['venueSourceURL'] = response.css('div.venue-info-details').xpath('.//a/@href').extract()[0]
            item['venueSourceURL'] = urljoin(self.domain, item['venueSourceURL'])
            item['venueSourceRef'] = item['venueSourceURL'].split('/')[4].split('-')[0]
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


        ## Venue Name
        try:
            item['venueName'] = response.css('h1.h0::text').extract()[0].strip().strip('–').strip()
        except Exception as e:
            pass

        ## Venue Address
        try:
            list2 = response.css('p.venue-hcard').xpath('.//span/text()').extract()
            item['venueAddress'] = ', '.join([x for x in list2 if x.strip() != ''])
        except:
            pass

        ## Venue City
        try:
            item['venueCity'] = response.css('h1.h0').xpath('.//a/text()').extract()[0]
        except:
            pass

        ## Venue Website
        try:
            item['venueWebsite'] = response.css('p.venue-hcard').xpath('.//span/a/@href').extract()[0]
        except:
            pass



        yield item