import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import numpy as np

import csv
import MySQLdb
import os
import datetime
from scrapy.utils.project import get_project_settings
import logging

class InstagramApp():

    def __init__(self):
        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.logger = logging.getLogger("InstagramApp")
        self.open_spider()
        self.fetch_urls()
        self.close_spider()

    def open_spider(self):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self):
        self.cursor.close()

    def fetch_urls(self):
        try:
            query = 'SELECT artistID, url FROM WDJP.dj_artist_website where sourceID={}'.format(self.settings.get('INSTAGRAM_SOURCE_ID'))
            results = self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                artistID = row[0]
                instaURL = row[1]
                print(artistID,instaURL)
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (fetch_urls) Error %d: %s" % (e.args[0], e.args[1]))
        return False

    def get_insta_data(self,instaurl):
        results = {}
        try:
            r = requests.get(instaurl)
            self.logger.info('Processing: '+str(instaurl))
            soup = BeautifulSoup(r.text, 'html.parser')
            text = soup.find_all('script')[2].text.strip('window._sharedData = ').strip(';')
            data = json.loads(text)
            user = data['entry_data']['ProfilePage'][0]['user']
            results['user'] = user
            results['followed_by'] = user['followed_by']['count']
            results['follows'] = user['follows']['count']
            results['external_url'] = user.get('external_url',None)
            results['full_name'] = user['full_name']
            results['profile_pic_url'] = user['profile_pic_url']
            results['username'] = user['username']
            results['biography'] = user['biography']
            results['account_id'] = user['id']
            results['num_posts'] = user['media']['count']
        except Exception as e:
            print('Error in ' + str(instaurl))

        return results


if __name__ == '__main__':
    InstagramApp()

