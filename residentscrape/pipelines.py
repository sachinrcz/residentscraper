# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import MySQLdb
import os
import datetime



class ResidentscrapePipeline(object):

    def open_spider(self,spider):
        self.file = open('ResidentScrape.csv','w')
        self.csvwriter = csv.writer(self.file)
        self.headers = ["Event Name",'Event Date','Event Timing',"Venue Name","Venue Address","Cost","Minimum Age","Promoters","Memeber Attending","Lineup","Details","EventURL","Promoter Name","Promoter Address","Promoter Phone","Promoter Email","Promoter Website","Promoter Facebook","Promoter Twitter","Promoter Instagram","PromoterURL","Promoter Region"]
        self.keys = ['eventname','eventdate','eventtime','venuename','venueadd','cost','age','promoter','members','lineup','eventdetail','eventurl','promoterName','promoterAdd','promoterPhone','promoterEmail','promoterWebsite','promoterFacebook','promoterTwitter','promoterInstagram','promoterURL','promoterRegion']
        self.csvwriter.writerow(self.headers)


    def process_item(self, item, spider):
        self.csvwriter.writerow([item[key] for key in self.keys])

        return item


    def close_spider(self,spider):
        self.file.close()

class ArtistscrapePipeline(object):

    def open_spider(self,spider):
        self.file = open('EventScrape.csv','w')
        self.csvwriter = csv.writer(self.file)
        self.headers = ["Event Name", 'Event Start Date','Event End Date', 'Event Timing', "Venue Name", "Venue Address", "Cost",
                        "Minimum Age", "Promoters", "Memeber Attending", "Lineup", "Details", "EventURL",
                        "Promoter Name", "Promoter Address", "Promoter Phone", "Promoter Email", "Promoter Website",
                        "Promoter Facebook", "Promoter Twitter", "Promoter Instagram", "PromoterURL", "Promoter Region"]
        self.keys = ['eventname', 'eventstartdate','eventenddate', 'eventtime', 'venuename', 'venueadd', 'cost', 'age', 'promoter',
                     'members', 'lineup', 'eventdetail', 'eventurl', 'promoterName', 'promoterAdd', 'promoterPhone',
                     'promoterEmail', 'promoterWebsite', 'promoterFacebook', 'promoterTwitter', 'promoterInstagram',
                     'promoterURL', 'promoterRegion']
        self.csvwriter.writerow(self.headers)


    def process_item(self, item, spider):
        self.csvwriter.writerow([item[key] for key in self.keys])

        return item


    def close_spider(self,spider):
        self.file.close()


class MySQLStorePipeLine(object):

    def __init__(self,sqlusername,sqlpassword, db, host):
        self.sqlusername = sqlusername
        self.sqlpassword = sqlpassword
        self.db = db
        self.host = host

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlusername=crawler.settings.get('SQLUSERNAME'),
            sqlpassword=os.environ.get('SECRET_KEY'),
            db = crawler.settings.get('DATABASE'),
            host = crawler.settings.get('HOST'),
        )

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        try:
            item['eventStartDate'] =  datetime.datetime.strptime(item['eventStartDate'].strip(), '%d %b %Y').date()
            item['eventEndDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%d %b %Y').date()

            self.cursor.execute("""INSERT INTO dj_Events_ResidentEvent (
                                eventName, eventStartDate, eventEndDate,
                                eventTiming, eventVenueName, eventVenueAddress, 
                                cost, minimumAge, promoters, membersAttending,
                                lineup, details, eventURL, promoterName,
                                promoterAddress, promoterPhone, promoterEmail,
                                promoterWebsite, promoterFacebook, promoterTwitter,
                                promoterInstagram,promoterURL, promoterRegion
                                    )  
                                VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s                                 
                                 )""",
                                (item['eventName'].encode('utf-8'),
                                 item['eventStartDate'],
                                 item['eventEndDate'],
                                 item['eventTime'].encode('utf-8'),
                                 item['venueName'].encode('utf-8'),
                                 item['venueAdd'].encode('utf-8'),
                                 item['cost'].encode('utf-8'),
                                 item['age'].encode('utf-8'),
                                 item['promoter'].encode('utf-8'),
                                 item['members'].encode('utf-8'),
                                 item['lineup'].encode('utf-8'),
                                 item['eventDetail'].encode('utf-8'),
                                 item['eventURL'].encode('utf-8'),
                                 item['promoterName'].encode('utf-8'),
                                 item['promoterAdd'].encode('utf-8'),
                                 item['promoterPhone'].encode('utf-8'),
                                 item['promoterEmail'].encode('utf-8'),
                                 item['promoterWebsite'].encode('utf-8'),
                                 item['promoterFacebook'].encode('utf-8'),
                                 item['promoterTwitter'].encode('utf-8'),
                                 item['promoterInstagram'].encode('utf-8'),
                                 item['promoterURL'].encode('utf-8'),
                                 item['promoterRegion'].encode('utf-8'),
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

        return item


class ArtistSQLPipeLine(object):

    def __init__(self,sqlusername,sqlpassword, db, host, sourceID):
        self.sqlusername = sqlusername
        self.sqlpassword = sqlpassword
        self.db = db
        self.host = host
        self.sourceID = sourceID

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlusername=crawler.settings.get('SQLUSERNAME'),
            sqlpassword=os.environ.get('SECRET_KEY'),
            db = crawler.settings.get('DATABASE'),
            host = crawler.settings.get('HOST'),
            sourceID = crawler.settings.get('sourceID')
        )

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO scrape_Artists (
                                sourceID, sourceArtistRef, linkedScrapeArtistID,
                                WPArtistID, name, realName, 
                                aliases, country, biography
                                    )  
                                VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s                             
                                 )""",
                                (self.sourceID,
                                 item['sourceRef'],
                                 0,
                                 0,
                                 item['name'].encode('utf-8'),
                                 item['realName'].encode('utf-8'),
                                 item['aliases'].encode('utf-8'),
                                 item['country'].encode('utf-8'),
                                 item['biography'].encode('utf-8')
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

        return item


