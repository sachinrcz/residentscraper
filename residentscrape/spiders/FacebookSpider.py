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
import facebook
import json


class FacebookSpider(scrapy.Spider):

    name = "FacebookSpider"

    domain = "https://graph.facebook.com"

    logger = logging.getLogger("FacebookSpider")

    project_settings = get_project_settings()

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1,
        "SOURCE_ID": project_settings['FACEBOOK_SOURCE_ID']
    }

    def start_requests(self):
        self.custom_settings = get_project_settings()
        ## Facebook API fields to retrieve
        self.eventFields = ['id','name','attending_count','description','attending','start_time','end_time','interested_count','place','ticket_uri']
        self.artistFields = ['id', 'about', 'bio', 'contact_address', 'current_location', 'events', 'fan_count', 'name',
                        'website']

        url = 'https://graph.facebook.com/oauth/access_token?client_id={}&client_secret={}&grant_type=client_credentials'.format(self.custom_settings['FACEBOOK_APP_ID'],self.custom_settings['FACEBOOK_APP_SECRET'])
        request = scrapy.Request(url=url, callback=self.parse)
        yield request

    def parse(self,response):
        ## Retrieve Access Token
        res = json.loads(response.text)
        self.logger.info("Graph Outh Response: "+str(res))
        self.access_token = res['access_token']
        self.logger.info('Access_Token: '+str(self.access_token))





        password = os.environ.get('SECRET_KEY')
        db = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'],
                             passwd=password, db=self.custom_settings['DATABASE'])
        cursor = db.cursor()

        ## Get Artist URL from old database
        query = "SELECT sourceID, url FROM dj_artist_website WHERE sourceID={} LIMIT 1000;".format(self.custom_settings['FACEBOOK_SOURCE_ID'])


        ## Fetch Facebook Links from New DB Artists Table
        # query = 'SELECT sourceArtistRef, facebook FROM WDJPNew.scrape_Artists WHERE length(facebook) > 0 and sourceID=2 LIMIT 150;'


        ## Fetch Facebook Links from Scrape Promoter
        # query = 'SELECT sourceArtistRef, facebook FROM WDJPNew.scrape_Artists WHERE length(facebook) > 0 and sourceID=2 LIMIT 1000;'

        cursor.execute(query)
        rows = cursor.fetchall()
        # urls = [row[0]+'/dates' for row in data]
        for row in rows:
            url = row[1]
            if 'https://www.facebook.com/' in url:
                try:
                    pageID = url.strip('https://www.facebook.com/').split('/')[0]
                    url = self.domain + "/v2.11/" + pageID + "?fields=" + ','.join(
                        self.artistFields) + "&access_token=" + self.access_token
                    request = scrapy.Request(url=url, callback=self.parse_artist)
                    request.meta['artistSourceRef'] = pageID
                    request.meta['facebook'] = row[1]
                    yield request
                except:
                    pass



        ## Test with one URL
        # fbUrl = 'https://www.facebook.com/fabriclondon'
        # if 'http' in fbUrl:
        #     try:
        #         pageID = fbUrl.split('/')[-1]
        #         url = self.domain + "/v2.11/" + pageID + "?fields=" + ','.join(
        #             self.artistFields) + "&access_token=" + self.access_token
        #         request = scrapy.Request(url=url, callback=self.parse_artist)
        #         request.meta['artistSourceRef'] = pageID
        #         request.meta['facebook'] = fbUrl
        #         yield request
        #     except:
        #         pass


    def parse_artist(self,response):
        item = ArtistItem()
        for field in item.fields:
            item.setdefault(field, '')

        item['sourceRef'] = response.meta['artistSourceRef']
        result = json.loads(response.text)
        item['sourceURL'] = response.url
        item['sourceText'] = response.text


        item['name'] = self.get_key(result,'name')
        try:
            item['followers'] = int(self.get_key(result,'fan_count'))
        except:
            pass
        item['biography'] = self.get_key(result,'bio')
        item['website'] = self.get_key(result,'website')
        item['facebook'] = response.meta['facebook']
        yield item

        events = []
        try:
            events = result['events']['data']
        except:
            pass
        item = None
        for event in events:
            if self.custom_settings['FACEBOOK_API_USE_MODE'] == 1:
                item = ResidentItem()
                for field in item.fields:
                    item.setdefault(field, '')

                for field in item.fields:
                    item.setdefault(field, '')
                item['venueCapacity'] = 0
                item['eventFollowers'] = 0
                item['venueFollowers'] = 0
                item['promoterFollowers'] = 0
                item['scrapeVenueID'] = -1
                item['venueGeoLat'] = None
                item['venueGeoLong'] = None
                item['venueSourceRef'] = '-1'
                item['eventSourceURL'] = response.url
                item['eventSourceText'] = str(event)
                item['venueSourceURL'] = response.url
                item['venueSourceText'] = str(event)
                item['artistSourceRef'] = response.meta['artistSourceRef']
                result = event

                try:
                    item['eventSourceRef'] = result['id']
                except:
                    pass
                try:
                    item['eventName'] = result['name']
                except:
                    pass
                try:
                    item['eventFollowers'] = result['attending_count']
                except:
                    pass
                try:
                    item['eventDescription'] = result['description']
                except:
                    pass
                try:
                    item['eventStartDate'] = result['start_time']
                except:
                    pass
                try:
                    item['eventEndDate'] = result['end_time']
                except:
                    pass
                try:
                    item['eventStartTime'] = item['eventStartDate'][:-5].split('T')[1]
                except:
                    pass
                try:
                    item['eventEndTime'] = item['eventEndDate'][:-5].split('T')[1]
                except:
                    pass


                ## convert to MySQL Dates
                try:
                    item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].split('T')[0],
                                                                   '%Y-%m-%d').date()
                except:
                    item['startDate']= None
                    pass
                try:
                    item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].split('T')[0], '%Y-%m-%d').date()
                except:
                    item['endDate'] = None
                    pass

                ## Parse Venue Information

                place = self.get_key(result, 'place')
                try:
                    item['venueSourceText'] = str(place)
                except:
                    pass
                try:
                    item['venueSourceRef'] = place['id']
                except:
                    pass
                try:
                    item['venueName'] = place['name']
                except:
                    pass
                try:
                    location = place['location']
                    try:
                        item['venueStreet'] = location['street']
                    except:
                        pass
                    try:
                        item['venueCity'] = location['city']
                    except:
                        pass
                    try:
                        item['venueCountry'] = location['country']
                    except:
                        pass
                    try:
                        item['venueState'] = location['state']
                    except:
                        pass
                    try:
                        item['venueZip'] = location['zip']
                    except:
                        pass
                    try:
                        item['venueGeoLat'] = location['latitude']
                    except:
                        pass
                    try:
                        item['venueGeoLong'] = location['longitude']
                    except:
                        pass
                except:
                    pass

                yield item
            else:
                try:
                    eventID = event['id']
                    url = self.domain + "/v2.11/" + eventID + "?fields=" + ','.join(
                        self.eventFields) + "&access_token=" + self.access_token
                    request = scrapy.Request(url=url, callback=self.parse_event)
                    request.meta['artistSourceRef'] = item['sourceRef']
                    yield request
                except:
                    pass




    def parse_event(self,response=None):
        self.logger.info("Inside Event")
        item = ResidentItem()
        for field in item.fields:
            item.setdefault(field, '')

        for field in item.fields:
            item.setdefault(field, '')
        item['venueCapacity'] = 0
        item['eventFollowers'] = 0
        item['venueFollowers'] = 0
        item['promoterFollowers'] = 0
        item['scrapeVenueID'] = -1
        item['venueGeoLat'] = None
        item['venueGeoLong'] = None
        item['venueSourceRef'] = '-1'
        item['eventSourceURL'] = response.url
        item['eventSourceText'] = response.text
        item['venueSourceURL'] = response.url
        item['venueSourceText'] = response.text
        item['artistSourceRef'] = response.meta['artistSourceRef']
        result = json.loads(response.text)

        try:
            item['eventSourceRef'] = result['id']
        except:
            pass
        try:
            item['eventName'] = result['name']
        except:
            pass
        try:
            item['eventFollowers'] = result['attending_count']
        except:
            pass
        try:
            item['eventDescription'] = result['description']
        except:
            pass
        try:
            item['eventStartDate'] = result['start_time']
        except:
            pass
        try:
            item['eventEndDate'] = result['end_time']
        except:
            pass
        try:
            item['eventStartTime'] = item['eventStartDate'][:-5].split('T')[1]
        except:
            pass
        try:
            item['eventEndTime'] = item['eventEndDate'][:-5].split('T')[1]
        except:
            pass

        ## convert to MySQL Dates
        try:
            item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].split('T')[0],
                                                           '%Y-%m-%d').date()
        except:
            item['startDate'] = None
            pass
        try:
            item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].split('T')[0], '%Y-%m-%d').date()
        except:
            item['endDate'] = None
            pass

        ## Parse Venue Information


        place = self.get_key(result,'place')
        try:
            item['venueSourceText'] = str(place)
        except:
            pass
        try:
            item['venueSourceRef'] = place['id']
        except:
            pass
        try:
            item['venueName'] = place['name']
        except:
            pass
        try:
            location = place['location']
            try:
                item['venueAddress'] = location['street']
            except:
                pass
            try:
                item['venueCity'] = location['city']
            except:
                pass
            try:
                item['venueCountry'] = location['country']
            except:
                pass
            try:
                item['venueGeoLat'] = location['latitude']
            except:
                pass
            try:
                item['venueGeoLong'] = location['longitude']
            except:
                pass
        except:
            pass



        yield item

    def get_key(self,result,key):
        if key in result.keys():
            return result[key]
        return ''