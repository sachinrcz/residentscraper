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
from  .items import GoogleMapItem, InstagramItem
from scrapy.utils.project import get_project_settings
import logging
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

    def __init__(self,crawlerSetting):
        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.sourceID = crawlerSetting.get('SOURCE_ID')
        self.logger = logging.getLogger("ArtistSQLPipeline")
        # extended data type mapping
        self.extendedTypeArtist = {'discogs':'discog','soundcloud':'soundcloud','bandcamp':'bandcamp'}

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings)

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        if not isinstance(item, ArtistItem):
            return item
        if not self.isArtist(item):
            self.insert_artist(item)
        else:
            self.update_artist(item)

        self.process_artist_extended_data(item)
        self.process_similar_artist_bit(item)
        return item


    def isArtist(self,item):
        try:
            query = "SELECT artistID FROM scrape_Artists WHERE sourceArtistRef=%s and sourceID=%s"
            args = (str(item['sourceRef']),self.sourceID)
            results = self.cursor.execute(query,args)
            if results > 0:
                data = self.cursor.fetchone()
                item['artistID'] = data[0]
                return True
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (isArtist) Error %d: %s" % (e.args[0], e.args[1]))
        return False

    def insert_artist(self,item):
        now = datetime.datetime.now()
        try:
            query = """INSERT INTO scrape_Artists (
                                        sourceID, sourceArtistRef,
                                        name, realName, 
                                        aliases, country, biography, followers,
                                        website, facebook, twitter, instagram,
                                        image_url,
                                        sourceURL, sourceText,  
                                        created
                                            )  
                                        VALUES (
                                            %s, %s,
                                            %s, %s, 
                                            %s, %s, %s, %s,
                                            %s, %s, %s, %s,
                                            %s,
                                            %s, %s, 
                                            %s                               
                                         )"""
            args  = (self.sourceID,
                                 item['sourceRef'],
                                 item['name'].encode('utf-8'),
                                 item['realName'].encode('utf-8'),
                                 item['aliases'].encode('utf-8'),
                                 item['country'].encode('utf-8'),
                                 item['biography'].encode('utf-8'),
                                 item['followers'],
                                 item['website'].encode('utf-8'),
                                 item['facebook'].encode('utf-8'),
                                 item['twitter'].encode('utf-8'),
                                 item['instagram'].encode('utf-8'),
                                 item['profile_pic_url'].encode('utf-8'),
                                 item['sourceURL'].encode('utf-8'),
                                 item['sourceText'].encode('utf-8'),
                                 now
                                 )
            self.cursor.execute(query,args)

            self.conn.commit()
            item['artistID'] = self.cursor.lastrowid

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_artist) Error %d: %s %s" % (e.args[0], e.args[1],item['sourceURL']))


    def update_artist(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Artists 
                                   SET sourceID=%s, sourceArtistRef=%s, linkedScrapeArtistID=%s,
                                   WPArtistID=%s, name=%s, realName=%s, 
                                   aliases=%s, country=%s, biography=%s,
                                   followers=%s, 
                                   website=%s, facebook=%s, twitter=%s, instagram=%s,image_url=%s,
                                   sourceURL=%s, sourceText=%s, 
                                   refreshed=%s WHERE artistID=%s """,
                                (self.sourceID,
                                 item['sourceRef'],
                                 0,
                                 0,
                                 item['name'].encode('utf-8'),
                                 item['realName'].encode('utf-8'),
                                 item['aliases'].encode('utf-8'),
                                 item['country'].encode('utf-8'),
                                 item['biography'].encode('utf-8'),
                                 item['followers'],
                                 item['website'].encode('utf-8'),
                                 item['facebook'].encode('utf-8'),
                                 item['twitter'].encode('utf-8'),
                                 item['instagram'].encode('utf-8'),
                                 item['profile_pic_url'].encode('utf-8'),
                                 item['sourceURL'].encode('utf-8'),
                                 item['sourceText'].encode('utf-8'),

                                 # item['website'].encode('utf-8'),
                                 # item['email'].encode('utf-8'),
                                 # item['facebook'].encode('utf-8'),
                                 # item['soundcloud'].encode('utf-8'),
                                 # item['twitter'].encode('utf-8'),
                                 # item['instagram'].encode('utf-8'),
                                 # item['discog'].encode('utf-8'),
                                 # item['bandcamp'].encode('utf-8'),
                                 now,
                                 item['artistID']
                                 ))

            self.conn.commit()
            # self.logger.info(str(self.cursor._last_executed))


        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_artist) Error %d: %s" % (e.args[0], e.args[1]))

    ### Handle Extended Data for Artist ###

    def process_artist_extended_data(self,item):

        for key,value in self.extendedTypeArtist.items():
            extended_eligible = False
            try:
                if len(item[value]) > 0:
                    extended_eligible = True
            except:
                if item[value] != 0:
                    extended_eligible = True

            if extended_eligible:
                extendedDataTypeID, extendedDataID = self.check_if_extended_exist(item,key)
                if extendedDataID == 0:
                    self.insert_artist_extended_data(item, key,  extendedDataTypeID)
                else:
                    self.update_artist_extended_data(item,key, extendedDataTypeID, extendedDataID)

    def check_if_extended_exist(self,item, key):
        extendedDataTypeID = 0
        extendedDataID = 0
        try:
            sql = "SELECT extendedDataTypeID from extended_data_type WHERE extendedDataTypeName='{}';".format(key)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                extendedDataTypeID = data[0]

            sql = "SELECT extendedDataID FROM extended_data WHERE extendedDataSourceType=1 and extendedDataSourceID={} and extendedDataTypeID={} ;".format(item['artistID'],extendedDataTypeID)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                extendedDataID = data[0]
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (check_if_extended_exist) Error %d: %s" % (e.args[0], e.args[1]))
        return extendedDataTypeID,extendedDataID

    def insert_artist_extended_data(self,item, key, extendedDataTypeID):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO extended_data (
                                                extendedDataSourceType, extendedDataSourceID, 
                                                extendedDataTypeID, extendedData1, 
                                                created
                                                    )  
                                                VALUES (
                                                    %s, %s, 
                                                    %s, %s, 
                                                    %s                               
                                                 )""",
                                (1,
                                 item['artistID'],
                                 extendedDataTypeID,
                                 item[self.extendedTypeArtist[key]],
                                 now
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error(self.cursor._last_executed)
            self.logger.error("Method: (insert_artist_extended_data) Error %d: %s" % (e.args[0], e.args[1]))


    def update_artist_extended_data(self,item,key, extendedDataTypeID, extendedDataID ):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE extended_data 
                                   SET extendedDataSourceType=%s, extendedDataSourceID=%s, 
                                   extendedDataTypeID=%s, extendedData1=%s, 
                                   refreshed=%s WHERE extendedDataID=%s""",
                                (1,
                                 item['artistID'],
                                 extendedDataTypeID,
                                 item[self.extendedTypeArtist[key]],
                                 now,
                                 extendedDataID
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_artist_extended_date) Error %d: %s" % (e.args[0], e.args[1]))


    ### Handle Similar Artist Data BandsInTown ###

    def process_similar_artist_bit(self,item):
        try:
            if len(item['similarArtists']) > 0 :
                for key,value in item['similarArtists'].items():
                    if not self.check_if_similar_artist_exist(item,key,value):
                        self.insert_similar_artist_bit(item,key,value)

        except Exception as e:
            self.logger.error("Method: (process_similar_artist_bit) Error %d: %s" % (e.args[0], e.args[1]))



    def check_if_similar_artist_exist(self,item, scrapeSourceRef, scrapeSourceName):
        try:
            sql = 'SELECT * FROM scrape_SimilarArtist WHERE scrapeArtistID={} and sourceID={} and scrapeSourceName="{}" and scrapeSourceRef="{}" ;'.format(item['artistID'],self.sourceID, scrapeSourceName,scrapeSourceRef)
            results = self.cursor.execute(sql)
            if results > 0:
                return True

        except Exception as e:
            self.logger.error("Method: (check_if_similar_artist_exist) Error %d: %s" % (e.args[0], e.args[1]))
        return False

    def insert_similar_artist_bit(self,item,scrapeSourceRef, scrapeSourceName):
        now = datetime.datetime.now()
        try:
            # scrapeArtistRefID = None
            # sql = 'SELECT artistID FROM scrape_Artists WHERE sourceArtistRef="{}" and sourceID={};'.format(
            #     scrapeSourceRef, self.sourceID)
            # results = self.cursor.execute(sql)
            # if results > 0:
            # if item['artistID']:
                # data = self.cursor.fetchone()
                # scrapeArtistRefID = data[0]
            self.cursor.execute("""INSERT INTO scrape_SimilarArtist (
                        scrapeArtistID, sourceID, 
                        scrapeSourceName, scrapeSourceRef, 
                        scrapeArtistRefID, created
                            )  
                        VALUES (
                            %s, %s, 
                            %s, %s, 
                            %s, %s                            
                         )""",
                        (item['artistID'],
                         self.sourceID,
                         scrapeSourceName,
                         scrapeSourceRef,
                         # scrapeArtistRefID,
                         item['sourceRef'],
                         now
                         ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_similar_artist_bit) Error %d: %s" % (e.args[0], e.args[1]))

class EventSQLPipeLine(object):

    def __init__(self,crawlerSetting):

        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.sourceID = crawlerSetting.get('SOURCE_ID')
        self.logger = logging.getLogger("EventSQLPipeline")
        # extended data type mapping
        self.extendedTypeEvent = { 'promotional':'eventPromotional', 'twitter':'eventTwitter','venue_aka':'venueAka'}
        self.extendedDataSourceTypeDict = {}

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings)

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        self.spider = spider
        if not isinstance(item, ResidentItem):
            return item
        isEvent = self.isEvent(item)
        isVenue = self.isVenue(item)
        if not isVenue:
            self.insert_venue(item)
        else:
            self.update_venue(item)

        isVenue = self.isVenue(item)
        if not isEvent:
            self.insert_event(item)
        else:
            self.update_event(item)



        if not self.isPromoter(item):
            self.insert_promoter(item)
        else:
            self.update_promoter(item)

        if not self.check_event_promoter_map(item):
            self.insert_event_promoter_map(item)

        if not self.check_artist_event_map(item,spider):
            self.insert_artist_event_map(item)

        self.process_extended_data(item)

        return item

    ### Select Queries

    def isVenue(self,item):
        # if len(item['venueName']) == 0:
        #     return True
        if '-1' not in item['venueSourceRef']:
            try:
                sql = "SELECT scrapeVenueID FROM scrape_Venues WHERE sourceVenueRef={} and sourceID={};".format(item['venueSourceRef'],self.sourceID)
                results = self.cursor.execute(sql)
                if results > 0:
                    data = self.cursor.fetchone()
                    item['scrapeVenueID'] = data[0]
                    return True
            except:
                pass

        try:
            sql = "SELECT scrapeVenueID FROM scrape_Events WHERE sourceEventRef={} and sourceID={};".format(item['eventSourceRef'], self.sourceID)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                if data[0] is not None:
                    item['scrapeVenueID'] = data[0]
                    return True
        except:
            pass

        return False

    def isEvent(self,item):
        try:
            sql = "SELECT scrapeEventID FROM scrape_Events WHERE sourceEventRef={} and sourceID={};".format(item['eventSourceRef'], self.sourceID)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                item['scrapeEventID'] = data[0]
                return True
        except:
            pass
        return False

    def isPromoter(self,item):
        if len(item['promoterSourceRef']) == 0:
            return True
        try:
            sql = "SELECT scrapePromoterID FROM scrape_Promoters WHERE sourcePromoterRef=" + str(item['promoterSourceRef']) + ";"
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                item['scrapePromoterID'] = data[0]
                return True
        except:
            pass
        return False

    def check_artist_event_map(self,item,spider):
        try:
            query = 'SELECT artistID FROM scrape_Artists WHERE sourceArtistRef=%s and sourceID=%s'
            args = (str(item['artistSourceRef']),self.sourceID)
            results = self.cursor.execute(query,args)
            data = self.cursor.fetchone()
            item['scrapeArtistID'] = data[0]
            try:
                if len(item['scrapeEventID']) == 0:
                    sql = "SELECT scrapeEventID FROM scrape_Events WHERE sourceEventRef='" + str(item['eventSourceRef']) + "';"
                    results = self.cursor.execute(sql)
                    data = self.cursor.fetchone()
                    item['scrapeEventID'] = data[0]
            except:
                pass
            sql = "SELECT * FROM scrape_ArtistEvents WHERE scrapeArtistID=" + str(item['scrapeArtistID']) + " and scrapeEventID=" +  str(item['scrapeEventID']) + ";"
            results = self.cursor.execute(sql)
            if results > 0:
                return True
        except Exception as e:
            spider.log(str(e))
            spider.log(str(self.cursor._last_executed))
            pass
        return False

    def check_event_promoter_map(self,item):
        if len(item['promoterSourceRef']) == 0:
            return True
        try:
            sql = "SELECT * FROM scrape_Event_Promoter_Map WHERE scrapeEventID=" + str(item['scrapeEventID']) + " and scrapePromoterID=" +  str(item['scrapePromoterID']) + ";"
            results = self.cursor.execute(sql)
            if results > 0:
                return True
        except:
            pass
        return False


    ### Insert Queries

    def insert_event(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Events (
                                    sourceID, sourceEventRef,
                                    sourceVenueRef, scrapeVenueID, eventName, 
                                    startDateText, startTimeText, endDateText, endTimeText, startDate, endDate,
                                    description, lineup, facebook, eventAdmin, eventPromoters, eventVenueAddress, 
                                    followers, going,
                                    ticketinfo, price, ticketTier,ticketURL,
                                    sourceURL, sourceText, 
                                    created
                                        )  
                                    VALUES (
                                        %s, %s, 
                                        %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, 
                                        %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s,
                                        %s         
                                     )""",
                                (self.sourceID,
                                 item['eventSourceRef'].encode('utf-8'),
                                 item['venueSourceRef'].encode('utf-8'),
                                 item['scrapeVenueID'],
                                 item['eventName'].encode('utf-8'),
                                 item['eventStartDate'].encode('utf-8'),
                                 item['eventStartTime'].encode('utf-8'),
                                 item['eventEndDate'].encode('utf-8'),
                                 item['eventEndTime'].encode('utf-8'),
                                 item['startDate'],
                                 item['endDate'],
                                 item['eventDescription'].encode('utf-8'),
                                 item['eventLineup'].encode('utf-8'),
                                 item['eventFacebook'].encode('utf-8'),
                                 item['eventAdmin'].encode('utf-8'),
                                 item['eventPromoters'].encode('utf-8'),
                                 item['eventVenueAddress'].encode('utf-8'),
                                 item['eventFollowers'],
                                 item['eventGoing'],
                                 # item['eventPromotional'].encode('utf-8'),
                                 # item['eventTwitter'].encode('utf-8'),

                                 item['eventTicketInfo'].encode('utf-8'),
                                 item['eventTicketPrice'].encode('utf-8'),
                                 item['eventTicketTier'].encode('utf-8'),
                                 item['eventTicketURL'].encode('utf-8'),
                                 item['eventSourceURL'].encode('utf-8'),
                                 item['eventSourceText'].encode('utf-8'),
                                 now
                                 ))

            self.conn.commit()
            item['scrapeEventID'] = self.cursor.lastrowid

        except(MySQLdb.Error) as e:
            self.logger.error("Error occured while scraping: " + str(item['eventSourceURL']))
            self.logger.error("Method: (insert_event) Error %d: %s" % (e.args[0], e.args[1]))

    def insert_venue(self,item):

        if item['venueSourceRef'] == '-1' and item['venueTBAInsert'] == False:
            item['scrapeVenueID'] = -1
            return

        if (len(item['venueName']) == 0) and (len(item['venueAddress']) == 0):
            return
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Venues (
                                    sourceID, sourceVenueRef,venueName, 
                                    venueStreet, venueCity, venueState, venueRegion, venueZip, venueCountry, venueFullAddress,
                                    venueGeoLat, venueGeoLong,
                                    venuePhone, venueDescription, capacity,
                                    website, googleMaps, email, followers, 
                                    facebook, twitter, instagram, sourceURL, sourceText,
                                    created
                                    )  
                                    VALUES (
                                        %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s,
                                        %s, %s, %s,
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s,
                                        %s         
                                     )""",
                                (self.sourceID,
                                 item['venueSourceRef'].encode('utf-8'),
                                 item['venueName'].encode('utf-8'),
                                 item['venueStreet'].encode('utf-8'),
                                 item['venueCity'].encode('utf-8'),
                                 item['venueState'].encode('utf-8'),
                                 item['venueRegion'].encode('utf-8'),
                                 item['venueZip'].encode('utf-8'),
                                 item['venueCountry'].encode('utf-8'),
                                 item['venueAddress'].encode('utf-8'),
                                 item['venueGeoLat'],
                                 item['venueGeoLong'],
                                 # item['venueAka'].encode('utf-8'),
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
            item['scrapeVenueID'] = self.cursor.lastrowid

        except(MySQLdb.Error) as e:
            # self.logger.error(str(self.cursor._last_executed))
            self.logger.error("Error occured while scraping: " + str(item['venueSourceURL']))
            self.logger.error("Method: (insert_venue) Error %d: %s" % (e.args[0], e.args[1]))

    def insert_promoter(self,item):
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
                                 item['promoterSourceRef'].encode('utf-8'),
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


            item['scrapePromoterID'] = self.cursor.lastrowid
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_promoter) Error %d: %s" % (e.args[0], e.args[1]))

    def insert_artist_event_map(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_ArtistEvents (
                                            sourceID, scrapeArtistID, scrapeEventID,
                                            created
                                            )  
                                            VALUES (
                                                %s, %s, %s, 
                                                %s    
                                             )""",
                                (self.sourceID,
                                 item['scrapeArtistID'],
                                 item['scrapeEventID'],
                                 now
                                 ))

            self.conn.commit()
        except(MySQLdb.Error) as e:
            self.logger.error(str(self.cursor._last_executed))
            self.logger.error("Method: (insert_artist_event_map) Error %d: %s" % (e.args[0], e.args[1]))

    def insert_event_promoter_map(self, item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO scrape_Event_Promoter_Map (
                                            sourceID, scrapeEventID, scrapePromoterID,
                                            created
                                            )  
                                            VALUES (
                                                %s, %s, %s, 
                                                %s    
                                             )""",
                                (self.sourceID,
                                 item['scrapeEventID'],
                                 item['scrapePromoterID'],
                                 now
                                 ))

            self.conn.commit()
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_event_promoter_map) Error %d: %s" % (e.args[0], e.args[1]))


    ## Update Queries

    def update_event(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Events 
                                SET sourceID=%s, sourceEventRef=%s,
                                sourceVenueRef=%s, scrapeVenueID=%s, eventName=%s, 
                                startDateText=%s, startTimeText=%s, 
                                endDateText=%s, endTimeText=%s, startDate=%s, 
                                endDate=%s, description=%s, lineup=%s, facebook=%s, eventAdmin=%s, 
                                eventPromoters=%s, eventVenueAddress=%s, followers=%s, 
                                ticketinfo=%s, price=%s, ticketTier=%s,
                                sourceURL=%s, sourceText=%s,refreshed=%s 
                                WHERE scrapeEventID=%s""",
                                (self.sourceID,
                                 item['eventSourceRef'].encode('utf-8'),
                                 item['venueSourceRef'].encode('utf-8'),
                                 item['scrapeVenueID'],
                                 item['eventName'].encode('utf-8'),
                                 item['eventStartDate'].encode('utf-8'),
                                 item['eventStartTime'].encode('utf-8'),
                                 item['eventEndDate'].encode('utf-8'),
                                 item['eventEndTime'].encode('utf-8'),
                                 item['startDate'],
                                 item['endDate'],
                                 item['eventDescription'].encode('utf-8'),
                                 item['eventLineup'].encode('utf-8'),
                                 item['eventFacebook'].encode('utf-8'),
                                 item['eventAdmin'].encode('utf-8'),
                                 item['eventPromoters'].encode('utf-8'),
                                 item['eventVenueAddress'].encode('utf-8'),
                                 item['eventFollowers'],
                                 # item['eventPromotional'].encode('utf-8'),
                                 # item['eventTwitter'].encode('utf-8'),
                                 item['eventTicketInfo'].encode('utf-8'),
                                 item['eventTicketPrice'].encode('utf-8'),
                                 item['eventTicketTier'].encode('utf-8'),
                                 item['eventSourceURL'].encode('utf-8'),
                                 item['eventSourceText'].encode('utf-8'),
                                 now,
                                 item['scrapeEventID']
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_event) Error %d: %s" % (e.args[0], e.args[1]))
            # print("Error %d: %s" % (e.args[0], e.args[1]))

    def update_venue(self, item):
        if (len(item['venueName']) == 0) and (len(item['venueAddress']) == 0):
            return
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Venues 
                                   SET sourceID=%s, sourceVenueRef=%s, venueName=%s, 
                                   venueStreet=%s, venueCity=%s, venueState=%s, venueRegion=%s, venueZip=%s, venueCountry=%s, venueFullAddress=%s, 
                                   venueGeoLat=%s, venueGeoLong=%s,
                                   venuePhone=%s, venueDescription=%s, capacity=%s,
                                   website=%s, googleMaps=%s, email=%s, followers=%s, 
                                   facebook=%s, twitter=%s, instagram=%s, sourceURL=%s, 
                                   sourceText=%s, refreshed=%s
                                   WHERE scrapeVenueID=%s """,
                                (self.sourceID,
                                 item['venueSourceRef'].encode('utf-8'),
                                 item['venueName'].encode('utf-8'),
                                 item['venueStreet'].encode('utf-8'),
                                 item['venueCity'].encode('utf-8'),
                                 item['venueState'].encode('utf-8'),
                                 item['venueRegion'].encode('utf-8'),
                                 item['venueZip'].encode('utf-8'),
                                 item['venueCountry'].encode('utf-8'),
                                 item['venueAddress'].encode('utf-8'),
                                 item['venueGeoLat'],
                                 item['venueGeoLong'],
                                 # item['venueAka'].encode('utf-8'),
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
                                 now,
                                 item['scrapeVenueID']
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_venue) Error %d: %s" % (e.args[0], e.args[1]))

    def update_promoter(self, item):
        if len(item['promoterSourceRef']) == 0:
            return True
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Promoters 
                                   SET sourceID=%s, sourcePromoterRef=%s, 
                                   promoterName=%s, promoterAddress=%s, promoterRegion=%s, 
                                   promoterPhone=%s, followers=%s, website=%s, googleMap=%s, 
                                   email=%s,facebook=%s, twitter=%s, instagram=%s, 
                                   description=%s, sourceURL=%s, sourceText=%s,
                                   refreshed=%s WHERE scrapePromoterID=%s""",
                                (self.sourceID,
                                 item['promoterSourceRef'].encode('utf-8'),
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
                                 now,
                                 item['scrapePromoterID']
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_promoter) Error %d: %s" % (e.args[0], e.args[1]))


    ## Process Extended Data
    def process_extended_data(self, item):
        extendedDataSourceType, extendedDataSourceID_Key = 3, 'scrapeEventID'
        mappings = self.extendedTypeEvent
        for tableKey, itemKey in mappings.items():
            if len(item[itemKey]) >0:
                extendedDataTypeID, extendedDataID = self.check_if_extended_exist(item, tableKey,extendedDataSourceType )
                if extendedDataID == 0:
                    self.insert_extended_data(item, itemKey, extendedDataSourceType, extendedDataSourceID_Key, extendedDataTypeID)
                else:
                    self.update_extended_data(item, itemKey, extendedDataSourceType, extendedDataSourceID_Key, extendedDataTypeID, extendedDataID)

    def check_if_extended_exist(self, item, tableKey, extendedDataSourceType):
        extendedDataTypeID = 0
        extendedDataID = 0
        try:
            sql = "SELECT extendedDataTypeID from extended_data_type WHERE extendedDataTypeName='{}';".format(
                tableKey)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                extendedDataTypeID = data[0]

            sql = "SELECT extendedDataID FROM extended_data WHERE extendedDataSourceType={} and extendedDataSourceID={} and extendedDataTypeID={} ;".format(
                extendedDataSourceType, item['scrapeEventID'], extendedDataTypeID)
            results = self.cursor.execute(sql)
            if results > 0:
                data = self.cursor.fetchone()
                extendedDataID = data[0]
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (check_if_extended_exist) Error %d: %s" % (e.args[0], e.args[1]))
        return extendedDataTypeID, extendedDataID

    def insert_extended_data(self, item, itemKey, extendedDataSourceType,extendedDataSourceID_Key, extendedDataTypeID):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""INSERT INTO extended_data (
                                                   extendedDataSourceType, extendedDataSourceID, 
                                                   extendedDataTypeID, extendedData1, 
                                                   created
                                                       )  
                                                   VALUES (
                                                       %s, %s, 
                                                       %s, %s, 
                                                       %s                               
                                                    )""",
                                (extendedDataSourceType,
                                 item[extendedDataSourceID_Key],
                                 extendedDataTypeID,
                                 item[itemKey],
                                 now
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_extended_data) Error %d: %s" % (e.args[0], e.args[1]))

    def update_extended_data(self, item, itemKey, extendedDataSourceType, extendedDataSourceID_Key, extendedDataTypeID, extendedDataID):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE extended_data 
                                      SET extendedDataSourceType=%s, extendedDataSourceID=%s, 
                                      extendedDataTypeID=%s, extendedData1=%s, 
                                      refreshed=%s WHERE extendedDataID=%s""",
                                (extendedDataSourceType,
                                 item[extendedDataSourceID_Key],
                                 extendedDataTypeID,
                                 item[itemKey],
                                 now,
                                 extendedDataID
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_artist_extended_date) Error %d: %s" % (e.args[0], e.args[1]))

class GoogleSQLPipeLine(object):
    def __init__(self, crawlerSetting):
        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.sourceID = crawlerSetting.get('SOURCE_ID')
        self.logger = logging.getLogger("GoogleSQLPipeline")
        # extended data type mapping
        self.extendedTypeArtist = {'discogs': 'discog', 'soundcloud': 'soundcloud', 'bandcamp': 'bandcamp',
                                   'follows': 'follows', 'num_posts': 'num_posts', 'external_url': 'external_url',
                                   'profile_pic_url': 'profile_pic_url'}

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings)

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host,
                                    charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        if not isinstance(item, GoogleMapItem):
            return item
        if item['resultCount'] == 1:
            if not self.isExists(item):
                self.insert_google_address(item)
                self.insert_google_query(item)
            else:
                self.update_google_address(item)

        else:
            self.insert_google_query(item)

        self.update_venue_google_address_id(item)
        return item

    def isExists(self,item):
        try:
            query = "SELECT addressID FROM scrape_GoogleAddress WHERE sourceRef='{}'".format(item['sourceRef'])
            results = self.cursor.execute(query)
            if results > 0:
                data = self.cursor.fetchone()
                item['addressID'] = data['addressID']
                return True
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (isExists) Error %d: %s" % (e.args[0], e.args[1]))
        return False


    def update_google_address(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_GoogleAddress 
                                   SET sourceRef=%s, addressTypes=%s, formattedAddress=%s,
                                   street_address=%s, street_number=%s, route=%s, 
                                   intersection=%s, room=%s, floor=%s,
                                   post_box=%s, country=%s,
                                   administrative_area_level_1=%s, administrative_area_level_2=%s, administrative_area_level_3=%s, administrative_area_level_4=%s,administrative_area_level_5=%s,
                                   colloquial_area=%s, locality=%s, ward=%s, 
                                   sublocality=%s, neighborhood=%s, premise=%s,
                                   subpremise=%s, postal_code=%s,
                                   natural_feature=%s, airport=%s, park=%s, 
                                   point_of_interest=%s, longitude=%s, lattitude=%s,
                                   sourceURL=%s, sourceText=%s, 
                                   refreshed=%s WHERE addressID=%s """,
                                (
                                 item['sourceRef'].encode('utf-8'),
                                 item['address_types'],
                                 item['formatted_address'].encode('utf-8'),
                                 item['street_address'].encode('utf-8'),
                                 item['street_number'].encode('utf-8'),
                                 item['route'].encode('utf-8'),
                                 item['intersection'].encode('utf-8'),
                                 item['room'].encode('utf-8'),
                                 item['floor'].encode('utf-8'),
                                 item['post_box'].encode('utf-8'),
                                 item['country'].encode('utf-8'),
                                 item['administrative_area_level_1'].encode('utf-8'),
                                 item['administrative_area_level_2'].encode('utf-8'),
                                 item['administrative_area_level_3'].encode('utf-8'),
                                 item['administrative_area_level_4'].encode('utf-8'),
                                 item['administrative_area_level_5'].encode('utf-8'),
                                 item['colloquial_area'].encode('utf-8'),
                                 item['locality'].encode('utf-8'),
                                 item['ward'].encode('utf-8'),
                                 item['sublocality'].encode('utf-8'),
                                 item['neighborhood'].encode('utf-8'),
                                 item['premise'].encode('utf-8'),
                                 item['subpremise'].encode('utf-8'),
                                 item['postal_code'].encode('utf-8'),
                                 item['natural_feature'].encode('utf-8'),
                                 item['airport'].encode('utf-8'),
                                 item['park'].encode('utf-8'),
                                 item['point_of_interest'].encode('utf-8'),
                                 item['longitude'],
                                 item['lattitude'],
                                 item['sourceURL'].encode('utf-8'),
                                 item['sourceText'],
                                 now,
                                 item['addressID']
                                 ))

            self.conn.commit()


        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_google_address) Error %d: %s" % (e.args[0], e.args[1]))

    def insert_google_address(self,item):
        now = datetime.datetime.now()
        try:
            query = """INSERT INTO scrape_GoogleAddress (
                                   sourceRef, addressTypes, formattedAddress, street_address, street_number, route, intersection, room, floor, post_box, country,
                                   administrative_area_level_1, administrative_area_level_2, administrative_area_level_3, administrative_area_level_4,administrative_area_level_5,
                                   colloquial_area, locality, ward, sublocality, neighborhood, premise, subpremise, postal_code, natural_feature, airport, park, 
                                   point_of_interest, longitude, lattitude,
                                   sourceURL, sourceText, 
                                   created
                                    )  
                                    VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                     )"""

            args = (item['sourceRef'].encode('utf-8'),
                    str(item['address_types']),
                    item['formatted_address'].encode('utf-8'),
                    item['street_address'].encode('utf-8'),
                    item['street_number'].encode('utf-8'),
                    item['route'].encode('utf-8'),
                    item['intersection'].encode('utf-8'),
                    item['room'].encode('utf-8'),
                    item['floor'].encode('utf-8'),
                    item['post_box'].encode('utf-8'),
                    item['country'].encode('utf-8'),
                    item['administrative_area_level_1'].encode('utf-8'),
                    item['administrative_area_level_2'].encode('utf-8'),
                    item['administrative_area_level_3'].encode('utf-8'),
                    item['administrative_area_level_4'].encode('utf-8'),
                    item['administrative_area_level_5'].encode('utf-8'),
                    item['colloquial_area'].encode('utf-8'),
                    item['locality'].encode('utf-8'),
                    item['ward'].encode('utf-8'),
                    item['sublocality'].encode('utf-8'),
                    item['neighborhood'].encode('utf-8'),
                    item['premise'].encode('utf-8'),
                    item['subpremise'].encode('utf-8'),
                    item['postal_code'].encode('utf-8'),
                    item['natural_feature'].encode('utf-8'),
                    item['airport'].encode('utf-8'),
                    item['park'].encode('utf-8'),
                    item['point_of_interest'].encode('utf-8'),
                    item['longitude'],
                    item['lattitude'],
                    item['sourceURL'].encode('utf-8'),
                    item['sourceText'],
                    now
                    )
            self.cursor.execute(query,args)

            self.conn.commit()
            item['addressID'] = self.cursor.lastrowid

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_google_address) Error %d: %s %s" % (e.args[0], e.args[1],item['sourceURL']))
            self.logger.error(self.cursor._last_executed)


    def insert_google_query(self,item):
        now = datetime.datetime.now()
        try:
            query = """INSERT INTO scrape_GoogleQueries (
                                                googleAddressID, query, 
                                                count, json, 
                                                created
                                                    )  
                                                VALUES (
                                                    %s, %s,%s, %s, %s                               
                                                 )"""
            args = (item['addressID'],
                    item['query'].encode('utf-8'),
                    item['resultCount'],
                    item['sourceText'],
                    now
                    )
            self.cursor.execute(query, args)

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_google_query) Error %d: %s %s" % (e.args[0], e.args[1], item['sourceURL']))

    def update_venue_google_address_id(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE scrape_Venues 
                                          SET googleAddressID=%s, refreshed=%s
                                            WHERE scrapeVenueID=%s""",
                                (
                                    item['addressID'],
                                    now,
                                    item['venueID']
                                 ))

            self.conn.commit()

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_venue_google_address_id) Error %d: %s" % (e.args[0], e.args[1]))

        pass

class InstagramPipeLine(object):

    def __init__(self,crawlerSetting):
        self.settings = get_project_settings()
        self.sqlusername = self.settings.get('SQLUSERNAME')
        self.sqlpassword = os.environ.get('SECRET_KEY')
        self.db = self.settings.get('DATABASE')
        self.host = self.settings.get('HOST')
        self.sourceID = crawlerSetting.get('SOURCE_ID')
        self.logger = logging.getLogger("InstagramSQLPipeline")

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings)

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(user=self.sqlusername, passwd=self.sqlpassword, db=self.db, host=self.host, charset="utf8mb4",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        if not isinstance(item, InstagramItem):
            return item
        self.getArtistID(item)
        if not self.isExist(item):
            self.insert_insta_data(item)
        else:
            self.update_insta_data(item)

        return item

    def getArtistID(self,item):
        try:
            query = "SELECT artistID FROM scrape_Artists WHERE sourceArtistRef=%s and sourceID=%s"
            args = (str(item['sourceRef']),self.sourceID)
            results = self.cursor.execute(query,args)
            if results > 0:
                data = self.cursor.fetchone()
                item['artistID'] = data[0]
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (isExist) Error %d: %s" % (e.args[0], e.args[1]))

    def isExist(self,item):
        try:
            query = "SELECT id FROM social_data_instagram WHERE sourceRef='{}'".format(str(item['sourceRef']))
            results = self.cursor.execute(query)
            if results > 0:
                data = self.cursor.fetchone()
                item['instaID'] = data[0]
                return True
        except(MySQLdb.Error) as e:
            self.logger.error("Method: (isExist) Error %d: %s" % (e.args[0], e.args[1]))
        return False

    def insert_insta_data(self,item):
        now = datetime.datetime.now()
        try:
            query = """INSERT INTO social_data_instagram (
                                        name, artistID, sourceRef, follows, num_posts,
                                        external_url, created
                                            )  
                                        VALUES ( 
                                            %s, %s, %s, %s,
                                            %s, %s, %s                              
                                         )"""
            args  = (
                                 item['name'].encode('utf-8'),
                                 item['artistID'],
                                 item['sourceRef'].encode('utf-8'),
                                 item['follows'],
                                 item['num_posts'],
                                 item['external_url'].encode('utf-8'),
                                 now
                                 )
            self.cursor.execute(query,args)

            self.conn.commit()
            item['instaID'] = self.cursor.lastrowid

        except(MySQLdb.Error) as e:
            self.logger.error("Method: (insert_insta_data) Error %d: %s %s" % (e.args[0], e.args[1],item['name']))


    def update_insta_data(self,item):
        now = datetime.datetime.now()
        try:
            self.cursor.execute("""UPDATE social_data_instagram 
                                   SET name=%s, artistID=%s, sourceRef=%s,
                                   follows=%s, num_posts=%s, external_url=%s, 
                                   refreshed=%s WHERE id=%s """,
                                (item['name'].encode('utf-8'),
                                 item['artistID'],
                                 item['sourceRef'].encode('utf-8'),
                                 item['follows'],
                                 item['num_posts'],
                                 item['external_url'].encode('utf-8'),
                                 now,
                                 item['instaID']
                                 ))

            self.conn.commit()
            # self.logger.info(str(self.cursor._last_executed))


        except(MySQLdb.Error) as e:
            self.logger.error("Method: (update_insta_data) Error %d: %s" % (e.args[0], e.args[1]))
