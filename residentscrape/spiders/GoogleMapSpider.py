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
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
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
        # query = "SELECT * FROM scrape_Venues where isTBA=0"
        query = "SELECT * FROM scrape_Venues where googleResultsCount>2 and googleAddressID = -1;"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.logger.info("Total Rows: "+str(len(rows)))
        for row in rows:

            query = None

            ## check for GoogleMap Links
            if len(row['googleMaps']) > 0 and 'http://maps.google.com/maps?' in row['googleMaps']:
                query = row['googleMaps'].strip('http://maps.google.com/maps?q=').lower()
            else:
                q1 = None

                columns = ['venueStreet', 'venueCity', 'venueState', 'venueZip', 'venueCountry']
                qdata = []

                for x in columns:
                    if len(row[x]) > 1:
                        qdata.append(row[x])
                q1 = ', '.join(qdata)

                if len(q1) > len(row['venueFullAddress']):
                    query = q1
                else:
                    query = row['venueFullAddress']

                # if row['sourceID'] == 1:
                query = row['venueName']+', '+ query




            if query is not None :

                ## Check if query in cache
                sqlquery = "SELECT * FROM scrape_GoogleQueries WHERE query =%s;"
                results = self.cursor.execute(sqlquery,[query])

                if results > 0 and row['googleResultsCount'] < 2:

                    self.logger.info('Query: {} already exist in cached data'.format(query))
                    data = self.cursor.fetchone()
                    self.update_venue_google_address_id(data['ID'],data['googleAddressID'],data['count'],row['scrapeVenueID'])

                else:
                    request = scrapy.Request(url=self.APIURL+query, callback=self.parse, dont_filter=True)
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
        item['queryID'] = -1

        item['query'] = response.meta['query']
        item['venueID'] = response.meta['venueID']
        data = json.loads(response.text)
        item['sourceText'] = data
        item['sourceURL'] = response.url
        item['resultCount'] = len(data['results'])

        if data['status'] == 'OK' and item['resultCount'] == 1:
            data = data['results'][0]
            self.extract_info(data,item)
        elif data['status'] == 'OK' and item['resultCount'] > 1:
            required_list = ['bar', 'club', 'stadium']
            for i in data['results']:
                if 'bar' in i['types'] or 'club' in i['types'] or 'stadium' in i['types'] or 'night_club' in i['types']:
                    data = i
                    self.extract_info(data, item)
                    break


        yield item

    def extract_info(self,data,item):
        item['address_types'] = data.get('types', '')
        item['formatted_address'] = data.get('formatted_address', '')
        item['sourceRef'] = data.get('place_id', '-1')

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
            item['longitude'] = geo.get('lng', None)
            item['lattitude'] = geo.get('lat', None)
        except:
            pass


    def update_venue_google_address_id(self,googleQueryID, googleAddressID,googleResultsCount, scrapeVenueID):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Venues
                                          SET googleAddressID=%s,
                                          googleQueryID=%s,
                                           googleResultsCount=%s, refreshed=%s
                                            WHERE scrapeVenueID=%s""",
                                (
                                    googleAddressID,
                                    googleQueryID,
                                    googleResultsCount,
                                    now,
                                    scrapeVenueID
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_venue_google_address_id) Error %d: %s" % (e.args[0], e.args[1]))

        pass


