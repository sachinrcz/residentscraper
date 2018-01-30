import scrapy
from residentscrape.items import GoogleMapItem
import MySQLdb
import os
import datetime
from scrapy.utils.project import get_project_settings
import logging
import json
# from postal.parser import parse_address

class GoogleMapSpider(scrapy.Spider):
    name = "GoogleMapSpider"

    domain = "https://maps.googleapis.com"

    logger = logging.getLogger("GoogleMapSpider")

    project_settings = get_project_settings()

    custom_settings = {
        # "AUTOTHROTTLE_ENABLED": True,
        # "AUTOTHROTTLE_START_DELAY": 1,
        # "AUTOTHROTTLE_MAX_DELAY": 2,
        # "AUTOTHROTTLE_TARGET_CONCURRENCY": 5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        # "DOWNLOAD_DELAY": 1,
        "SOURCE_ID": project_settings['GOOGLEMAP_SOURCE_ID']
    }


    def start_requests(self):
        self.custom_settings = get_project_settings()
        self.APIURL = 'https://maps.googleapis.com/maps/api/geocode/json?key={}&address='.format(self.custom_settings['GOOGLE_API_KEY'])
        ## Get URLs from SQL
        password = os.environ.get('SECRET_KEY')
        self.conn = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306, user=self.custom_settings['SQLUSERNAME'],
                             passwd=password, db=self.custom_settings['DATABASE'])
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM scrape_Venues where sourceVenueRef <> -1  LIMIT 5000;"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:

            query = None

            ## check for GoogleMap Links
            if len(row['googleMaps'])>0 and 'http://maps.google.com/maps?' in row['googleMaps']:
                query = row['googleMaps'].strip('http://maps.google.com/maps?q=').lower()
            elif len(row['venueFullAddress'])> 1:
                query = row['venueFullAddress'].strip()
            else:
                columns = ['venueStreet', 'venueCity', 'venueState', 'venueZip', 'venueCountry']
                qdata = []
                if row['sourceID'] == 1:
                    qdata.append(row['venueName'].strip())

                for x in columns:
                    if len(row[x])>1:
                        qdata.append(row[x])
                query = ', '.join(qdata)



            if query is not None:

                ## Check if query in cache
                sqlquery = 'SELECT * FROM scrape_GoogleQueries WHERE query = "{}";'.format(query)
                results = self.cursor.execute(sqlquery)

                if results>0:

                    self.logger.info('Query: {} already exist in cached data'.format(query))
                    data = self.cursor.fetchone()
                    self.update_venue_google_address_id(data['googleAddressID'],row['scrapeVenueID'])

                else:
                    request = scrapy.Request(url=self.APIURL+query, callback=self.parse)
                    request.meta['query'] = query
                    request.meta['venueID'] = row['scrapeVenueID']
                    yield request

    def parse(self, response):

        item = GoogleMapItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['longitude'] = None
        item['lattitude'] = None
        item['addressID'] = -1


        item['query'] = response.meta['query']
        item['venueID'] = response.meta['venueID']
        data = json.loads(response.text)
        item['sourceText'] = data
        item['sourceURL'] = response.url
        item['resultCount'] = len(data['results'])

        if data['status'] == 'OK' or item['resultCount'] == 1:
        #     self.update_venue_google_address_id(item['addressID'],item['venueID'])
        # else:
            data = data['results'][0]
            item['address_types'] = data.get('types','')
            item['formatted_address'] = data.get('formatted_address','')
            item['sourceRef'] = data.get('place_id','-1')

            try:
                for x in data['address_components']:
                    key = x['types'][0]
                    if key in item:
                        item[key] = x['long_name']
            except Exception as e:
                self.logger.error(e)

            ## Extract Geo Cordinates
            try:
                geo = data['geometry']['location']
                item['longitude'] = geo.get('lng',None)
                item['lattitude'] = geo.get('lat', None)
            except:
                pass





        yield item




    def update_venue_google_address_id(self,googleAddressID,scrapeVenueID):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Venues
                                          SET googleAddressID=%s, refreshed=%s
                                            WHERE scrapeVenueID=%s""",
                                (
                                    googleAddressID,
                                    now,
                                    scrapeVenueID
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_venue_google_address_id) Error %d: %s" % (e.args[0], e.args[1]))

        pass


