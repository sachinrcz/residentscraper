# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv

class ResidentscrapePipeline(object):

    def open_spider(self,spider):
        self.file = open('ResidentScrape.csv','w')
        self.csvwriter = csv.writer(self.file)
        self.headers = ["Event Name",'Event Date','Event Timing',"Venue Name","Venue Address","Cost","Minimum Age","Promoters","Memeber Attending","Lineup","Details","EventURL"]
        self.keys = ['eventname','eventdate','eventtime','venuename','venueadd','cost','age','promoter','members','lineup','eventdetail','eventurl']
        self.csvwriter.writerow(self.headers)


    def process_item(self, item, spider):
        self.csvwriter.writerow([item[key] for key in self.keys])

        return item


    def close_spider(self,spider):
        self.file.close()