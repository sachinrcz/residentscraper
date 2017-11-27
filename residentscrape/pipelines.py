# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv
# import json

class ResidentscrapePipeline(object):

    def open_spider(self,spider):
        self.file = open('ResidentScrape.csv','w')
        self.csvwriter = csv.writer(self.file)
        self.csvwriter.writerow(["Event Name",'Event Date','Event Timing'])


    def process_item(self, item, spider):
        self.csvwriter.writerow([item['eventname'],item['eventdate'],item['eventtime']])

        return item


    def close_spider(self,spider):
        self.file.close()