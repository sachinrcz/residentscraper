# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import MySQLdb
import os
import datetime
from .items import ArtistItem
from .items import ResidentItem


#
# class ResidentscrapePipeline(object):
#
#     def open_spider(self,spider):
#         self.file = open('ResidentScrape.csv','w')
#         self.csvwriter = csv.writer(self.file)
#         self.headers = ["Event Name",'Event Date','Event Timing',"Venue Name","Venue Address","Cost","Minimum Age","Promoters","Memeber Attending","Lineup","Details","EventURL","Promoter Name","Promoter Address","Promoter Phone","Promoter Email","Promoter Website","Promoter Facebook","Promoter Twitter","Promoter Instagram","PromoterURL","Promoter Region"]
#         self.keys = ['eventname','eventdate','eventtime','venuename','venueadd','cost','age','promoter','members','lineup','eventdetail','eventurl','promoterName','promoterAdd','promoterPhone','promoterEmail','promoterWebsite','promoterFacebook','promoterTwitter','promoterInstagram','promoterURL','promoterRegion']
#         self.csvwriter.writerow(self.headers)
#
#
#     def process_item(self, item, spider):
#         self.csvwriter.writerow([item[key] for key in self.keys])
#
#         return item
#
#
#     def close_spider(self,spider):
#         self.file.close()
#
# class ArtistscrapePipeline(object):
#
#     def open_spider(self,spider):
#         self.file = open('EventScrape.csv','w')
#         self.csvwriter = csv.writer(self.file)
#         self.headers = ["Event Name", 'Event Start Date','Event End Date', 'Event Timing', "Venue Name", "Venue Address", "Cost",
#                         "Minimum Age", "Promoters", "Memeber Attending", "Lineup", "Details", "EventURL",
#                         "Promoter Name", "Promoter Address", "Promoter Phone", "Promoter Email", "Promoter Website",
#                         "Promoter Facebook", "Promoter Twitter", "Promoter Instagram", "PromoterURL", "Promoter Region"]
#         self.keys = ['eventname', 'eventstartdate','eventenddate', 'eventtime', 'venuename', 'venueadd', 'cost', 'age', 'promoter',
#                      'members', 'lineup', 'eventdetail', 'eventurl', 'promoterName', 'promoterAdd', 'promoterPhone',
#                      'promoterEmail', 'promoterWebsite', 'promoterFacebook', 'promoterTwitter', 'promoterInstagram',
#                      'promoterURL', 'promoterRegion']
#         self.csvwriter.writerow(self.headers)
#
#
#     def process_item(self, item, spider):
#         self.csvwriter.writerow([item[key] for key in self.keys])
#
#         return item
#
#
#     def close_spider(self,spider):
#         self.file.close()
#
#
# class MySQLStorePipeLine(object):
#
#     def __init__(self,sqlusername,sqlpassword, db, host):
#         self.sqlusername = sqlusername
#         self.sqlpassword = sqlpassword
#         self.db = db
#         self.host = host
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             sqlusername=crawler.settings.get('SQLUSERNAME'),
#             sqlpassword=os.environ.get('SECRET_KEY'),
#             db = crawler.settings.get('DATABASE'),
#             host = crawler.settings.get('HOST'),
#         )
#
#     def open_spider(self, spider):
#         self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8",
#                                     use_unicode=True)
#         self.cursor = self.conn.cursor()
#
#     def close_spider(self, spider):
#         self.cursor.close()
#
#     def process_item(self, item, spider):
#         try:
#             item['eventStartDate'] =  datetime.datetime.strptime(item['eventStartDate'].strip(), '%d %b %Y').date()
#             item['eventEndDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%d %b %Y').date()
#
#             self.cursor.execute("""INSERT INTO dj_Events_ResidentEvent (
#                                 eventName, eventStartDate, eventEndDate,
#                                 eventTiming, eventVenueName, eventVenueAddress,
#                                 cost, minimumAge, promoters, membersAttending,
#                                 lineup, details, eventURL, promoterName,
#                                 promoterAddress, promoterPhone, promoterEmail,
#                                 promoterWebsite, promoterFacebook, promoterTwitter,
#                                 promoterInstagram,promoterURL, promoterRegion
#                                     )
#                                 VALUES (
#                                     %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                                     %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                                     %s, %s, %s, %s, %s
#                                  )""",
#                                 (item['eventName'].encode('utf-8'),
#                                  item['eventStartDate'],
#                                  item['eventEndDate'],
#                                  item['eventTime'].encode('utf-8'),
#                                  item['venueName'].encode('utf-8'),
#                                  item['venueAdd'].encode('utf-8'),
#                                  item['cost'].encode('utf-8'),
#                                  item['age'].encode('utf-8'),
#                                  item['promoter'].encode('utf-8'),
#                                  item['members'].encode('utf-8'),
#                                  item['lineup'].encode('utf-8'),
#                                  item['eventDetail'].encode('utf-8'),
#                                  item['eventURL'].encode('utf-8'),
#                                  item['promoterName'].encode('utf-8'),
#                                  item['promoterAdd'].encode('utf-8'),
#                                  item['promoterPhone'].encode('utf-8'),
#                                  item['promoterEmail'].encode('utf-8'),
#                                  item['promoterWebsite'].encode('utf-8'),
#                                  item['promoterFacebook'].encode('utf-8'),
#                                  item['promoterTwitter'].encode('utf-8'),
#                                  item['promoterInstagram'].encode('utf-8'),
#                                  item['promoterURL'].encode('utf-8'),
#                                  item['promoterRegion'].encode('utf-8'),
#                                  ))
#
#             self.conn.commit()
#
#
#         except(MySQLdb.Error) as e:
#             print("Error %d: %s" % (e.args[0], e.args[1]))
#
#         return item
#



# Main Artist Data pipeline
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
        if not isinstance(item, ArtistItem):
            return item
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Artists (
                                sourceID, sourceArtistRef, linkedScrapeArtistID,
                                WPArtistID, name, realName, 
                                aliases, country, biography,
                                sourceURL, sourceText, followers, 
                                website, email, facebook, soundcloud,
                                twitter, instagram, discogs, bandcamp,
                                created
                                    )  
                                VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s                               
                                 )""",
                                (self.sourceID,
                                 item['sourceRef'],
                                 0,
                                 0,
                                 item['name'].encode('utf-8'),
                                 item['realName'].encode('utf-8'),
                                 item['aliases'].encode('utf-8'),
                                 item['country'].encode('utf-8'),
                                 item['biography'].encode('utf-8'),
                                 item['sourceURL'].encode('utf-8'),
                                 item['sourceText'].encode('utf-8'),
                                 item['followers'],
                                 item['website'].encode('utf-8'),
                                 item['email'].encode('utf-8'),
                                 item['facebook'].encode('utf-8'),
                                 item['soundcloud'].encode('utf-8'),
                                 item['twitter'].encode('utf-8'),
                                 item['instagram'].encode('utf-8'),
                                 item['discog'].encode('utf-8'),
                                 item['bandcamp'].encode('utf-8'),
                                 now
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

        return item


class EventSQLPipeLine(object):

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
        if not isinstance(item, ResidentItem):
            return item
        if not self.isEvent(item):
            self.process_event(item)
            if not self.isVenue(item):
                self.process_venue(item)
            if not self.isPromoter(item):
                self.process_promoter(item)

        return item

    def process_event(self,item):
        now = datetime.datetime.now()
        try:
            try:
                item['startDate'] = datetime.datetime.strptime(item['eventStartDate'].strip(), '%d %b %Y').date()
            except:
                item['startDate'] = None
            try:
                item['endDate'] = datetime.datetime.strptime(item['eventEndDate'].strip(), '%d %b %Y').date()
            except:
                item['endDate'] = None
            self.cursor.execute("""INSERT INTO scrape_Events (
                                    sourceID, sourceEventRef,
                                    sourceVenueRef, eventName, 
                                    startDateText, startTimeText, endDateText, endTimeText, startDate, endDate,
                                    description, lineup, eventAdmin, eventPromoters, eventTBA, followers, 
                                    twitter, facebook,
                                    ticketinfo, price, ticketTier,
                                    sourceURL, sourceText, 
                                    created
                                        )  
                                    VALUES (
                                        %s, %s, 
                                        %s, %s, 
                                        %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, 
                                        %s, %s,
                                        %s, %s, %s,
                                        %s, %s,
                                        %s         
                                     )""",
                                (self.sourceID,
                                 item['eventSourceRef'],
                                 item['venueSourceRef'],
                                 item['eventName'].encode('utf-8'),
                                 item['eventStartDate'].encode('utf-8'),
                                 item['eventStartTime'].encode('utf-8'),
                                 item['eventEndDate'].encode('utf-8'),
                                 item['eventEndTime'].encode('utf-8'),
                                 item['startDate'],
                                 item['endDate'],
                                 item['eventDescription'].encode('utf-8'),
                                 item['eventLineup'].encode('utf-8'),
                                 item['eventAdmin'].encode('utf-8'),
                                 item['eventPromoters'].encode('utf-8'),
                                 item['eventTBA'].encode('utf-8'),
                                 item['eventFollowers'],
                                 item['eventTwitter'].encode('utf-8'),
                                 item['eventFacebook'].encode('utf-8'),
                                 item['eventTicketInfo'].encode('utf-8'),
                                 item['eventTicketPrice'].encode('utf-8'),
                                 item['eventTicketTier'].encode('utf-8'),
                                 item['eventSourceURL'].encode('utf-8'),
                                 item['eventSourceText'].encode('utf-8'),
                                 now
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

    def isVenue(self,item):
        if item['venueSourceRef'] == 0:
            return True
        try:
            sql = "SELECT * FROM scrape_Venues WHERE sourceVenueRef="+str(item['venueSourceRef'])+";"
            results = self.cursor.execute(sql)
            if results > 0:
                return True
        except:
            pass
        return False

    def isEvent(self,item):
        try:
            sql = "SELECT * FROM scrape_Events WHERE sourceEventRef=" + str(item['eventSourceRef']) + ";"
            results = self.cursor.execute(sql)
            if results > 0:
                return True
        except:
            pass
        return False

    def isPromoter(self,item):
        if item['promoterSourceRef'] == 0:
            return True
        try:
            sql = "SELECT * FROM scrape_Promoters WHERE sourcePromoterRef=" + str(item['promoterSourceRef']) + ";"
            results = self.cursor.execute(sql)
            if results > 0:
                return True
        except:
            pass
        return False

    def process_venue(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Venues (
                                    sourceID, sourceVenueRef, 
                                    venueName, venueAddress, venueCity, venueCountry,
                                    venueAka, venuePhone, venueDescription, capacity,
                                    website, googleMaps, email, followers, 
                                    facebook, twitter, instagram, sourceURL, sourceText,
                                    created
                                    )  
                                    VALUES (
                                        %s, %s, 
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s,
                                        %s         
                                     )""",
                                (self.sourceID,
                                 item['venueSourceRef'],
                                 item['venueName'].encode('utf-8'),
                                 item['venueAddress'].encode('utf-8'),
                                 item['venueCity'].encode('utf-8'),
                                 item['venueCountry'].encode('utf-8'),
                                 item['venueAka'].encode('utf-8'),
                                 item['venuePhone'].encode('utf-8'),
                                 item['venueDescription'].encode('utf-8'),
                                 item['venueCapacity'],
                                 item['venueWebsite'].encode('utf-8'),
                                 item['venueGoogleMap'].encode('utf-8'),
                                 item['venueEmail'].encode('utf-8'),
                                 item['venueFollowers'],
                                 item['venueFacebook'].encode('utf-8'),
                                 item['venueTwitter'].encode('utf-8'),
                                 item['venueInstagram'].encode('utf-8'),
                                 item['venueSourceURL'].encode('utf-8'),
                                 item['venueSourceText'].encode('utf-8'),
                                 now
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))


    def process_promoter(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Promoters (
                                    sourceID, sourcePromoterRef, 
                                    promoterName, promoterAddress, promoterRegion, 
                                    promoterPhone, followers, website, googleMap, 
                                    email,facebook, twitter, instagram, 
                                    description, sourceURL, sourceText,
                                    created
                                    )  
                                    VALUES (
                                        %s, %s, 
                                        %s, %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s, %s,
                                        %s         
                                     )""",
                                (self.sourceID,
                                 item['promoterSourceRef'],
                                 item['promoterName'].encode('utf-8'),
                                 item['promoterAddress'].encode('utf-8'),
                                 item['promoterRegion'].encode('utf-8'),
                                 item['promoterPhone'].encode('utf-8'),
                                 item['promoterFollowers'],
                                 item['promoterWebsite'].encode('utf-8'),
                                 item['promoterGoogleMap'].encode('utf-8'),
                                 item['promoterEmail'].encode('utf-8'),
                                 item['promoterFacebook'].encode('utf-8'),
                                 item['promoterTwitter'].encode('utf-8'),
                                 item['promoterInstagram'].encode('utf-8'),
                                 item['promoterDescription'].encode('utf-8'),
                                 item['promoterSourceURL'].encode('utf-8'),
                                 item['promoterSourceText'].encode('utf-8'),
                                 now
                                 ))

            self.conn.commit()

            self.cursor.execute("""INSERT INTO scrape_Event_Promoter_Map(
                                    eventSourceRef,promoterSourceRef, created
                                    )  
                                    VALUES (
                                        %s, %s, %s         
                                     )""",
                                (item['eventSourceRef'],
                                 item['promoterSourceRef'],
                                 now
                                 ))

            self.conn.commit()
        except(MySQLdb.Error) as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

