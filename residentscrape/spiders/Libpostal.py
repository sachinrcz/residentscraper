# from postal.parser import parse_address
from scrapy.utils.project import get_project_settings

import csv
import MySQLdb
import os
import datetime
import logging

class LibpostalScript():

    def __init__(self):
        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.logger = logging.getLogger("LibpostalLogger")
        self.open_spider()


    def open_spider(self):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host,
                                    charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def close_spider(self):
        self.cursor.close()

    def runLibpostal(self):
        try:
            query = "SELECT * FROM scrape_Venues"
            results = self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                address = row['venueFullAddress']
                if len(address) > 3:
                    lib_address = self.extract_address(address)
                    data = {}
                    data['venueID'] = row['scrapeVenueID']
                    data['venueCity'] = lib_address.get('city','')
                    data['venueState'] = lib_address.get('state','')
                    data['venueZip'] = lib_address.get('postal_code','')
                    data['venueCountry'] = lib_address.get('country','')
                    data['venueStreet'] = lib_address.get('address','')
                    self.update_venue(lib_address)

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (runLibpostal) Error %d: %s" % (e.args[0], e.args[1]))

        self.close_spider()

    def extract_address(self, address):
        data = {}
        res = parse_address(address)
        for item in res:
            data[item[1]] = item[0]
        mapping = ['house', 'house_number', 'road']
        data['address'] = ''
        for item in mapping:
            if item in data:
                data['address'] = data['address'].strip() + ' ' + data[item]
        if 'suburb' in data:
            data['address'] = data['address'].strip() + ', ' + data['suburb']
        if 'city_district' in data:
            data['address'] = data['address'].strip() + ', ' + data['city_district']
        data['address'] = data['address'].strip()

        return data

    def update_venue(self, item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Venues 
                                                  SET venueStreet=%s, venueCity=%s,
                                                  venueState=%s, venueZip=%s, venueCountry=%s,
                                                  refreshed=%s  WHERE scrapeVenueID=%s""",
                                (
                                    item['venueStreet'],
                                    item['venueCity'],
                                    item['venueState'],
                                    item['venueZip'],
                                    item['venueCountry'],
                                    now,
                                    item['venueID']
                                ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_venue) Error %d: %s" % (e.args[0], e.args[1]))

        pass