import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ResidentscrapeItem
import requests
import json
from bs4 import BeautifulSoup

class ResidentSpider(scrapy.Spider):

    name = "ResidentSpider"

    domain = "https://www.residentadvisor.net"

    def start_requests(self):
        r = requests.post('https://www.residentadvisor.net/Output/area-country-date-filter-handler.ashx?',
                          data={'type': 'getAreas', 'countryId': '30'})
        result = json.loads(r.text)
        soup = BeautifulSoup(result['data'], 'html.parser')
        cities = soup.find_all('li')
        cities = [tag.text for tag in cities]
        urls = ['/events/cn/'+city.lower()+'/month/2017-12-01' for city in cities ]
        # url = 'https://www.residentadvisor.net/events/cn/beijing/month/2017-12-01'
        for url in urls:
            request = scrapy.Request(url=self.domain+url, callback=self.parse)
            yield request


    def parse(self, response):
        events = response.xpath('//a[contains(@itemprop, "url")]/@href').extract()
        for event in events:
            request = scrapy.Request(url= self.domain + event,callback=self.parse_event)
            yield request


    def parse_event(self,response):
        item = ResidentscrapeItem()
        for field in item.fields:
            item.setdefault(field, '')
        item['eventurl'] = response.url
        item['eventname'] = response.css('div#sectionHead').xpath('.//h1/text()').extract()[0]
        details = response.css('aside#detail').xpath('.//li')
        for detail in details:
            list = detail.xpath('.//text()').extract()
            header = list[0]
            if 'Date' in header:
                if not (',' in list[1]):
                    item['eventdate'] = list[1] + ', '+list[2]
                    item['eventtime'] = list[3]
                continue

            if 'Venue' in header:
                item['venuename'] = list[1].strip()
                item['venueadd'] = list[2].strip()
                for i in range(3,len(list)):
                    item['venueadd'] = item['venueadd']+', '+list[i].replace('\xa0','').strip()
                continue

            if 'Cost' in header:
                item['cost'] = list[1].strip()
                continue

            if 'Promoter' in header:
                item['promoter'] = list[1].strip()
                continue

            if 'age' in header:
                item['age'] = list[1].strip()
                continue

        item['members'] = response.xpath('//h1[@id="MembersFavouriteCount"]/text()').extract()[0].replace('\n','').strip()
        temp = response.css('div.left').xpath('.//p')[0].xpath('.//text()').extract()
        item['lineup'] = (''.join(temp)).replace('\n','').strip()
        temp = response.css('div.left').xpath('.//p')[1].xpath('.//text()').extract()
        item['eventdetail'] = (''.join(temp)).replace('\n', '').strip()


        yield item
