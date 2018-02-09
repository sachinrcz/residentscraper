import scrapy
from residentscrape.items import GoogleMapItem
import MySQLdb
import os
import datetime
from scrapy.utils.project import get_project_settings
import logging
import json

class GooglePlaceSpider(scrapy.Spider):

    name = "GooglePlaceSpider"

    domain = "https://maps.googleapis.com"

    logger = logging.getLogger("GooglePlaceSpider")

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
        self.APIURL = 'https://maps.googleapis.com/maps/api/place/details/json?key={}&placeid='.format(
            self.custom_settings['GOOGLE_API_KEY'])

        ## Get URLs from SQL
        password = os.environ.get('SECRET_KEY')
        self.conn = MySQLdb.connect(host=self.custom_settings['HOST'], port=3306,
                                    user=self.custom_settings['SQLUSERNAME'],
                                    passwd=password, db=self.custom_settings['DATABASE'])
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        query = "SELECT * FROM wdjpdbcarling.scrape_GoogleAddress where placeName is null"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.logger.info("Total Rows: " + str(len(rows)))
        for row in rows:
            request = scrapy.Request(url=self.APIURL + row['sourceRef'], callback=self.parse, dont_filter=True)
            request.meta['placeID'] = row['sourceRef']
            request.meta['addressID'] = row['addressID']
            yield request

    def parse(self, response):

        id = response.meta['addressID']
        place_id = response.meta['placeID']
        self.logger.info('Source Ref: ' + str(place_id))
        data = json.loads(response.text)
        placeSourceText = data['result']
        name = None
        if 'name' in placeSourceText.keys():
            name = placeSourceText['name']
        self.update_google_address(id, name, placeSourceText)



    def update_google_address(self,addressID, placeName, placeSourceText):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_GoogleAddress
                                          SET placeName=%s,
                                          placeAPISourceText=%s, refreshed=%s
                                            WHERE addressID=%s""",
                                (

                                    placeName,
                                    placeSourceText,
                                    now,
                                    addressID
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_google_address) Error %d: %s" % (e.args[0], e.args[1]))

        pass
