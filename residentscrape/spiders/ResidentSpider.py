import scrapy
from scrapy.shell import inspect_response
from residentscrape.items import ResidentscrapeItem


class ResidentSpider(scrapy.Spider):

    name = "ResidentSpider"

    domain = "https://www.residentadvisor.net"

    def start_requests(self):
        urls = ['/events/cn/beijing/month/2017-12-01','/events/cn/beijing/month/2017-12-01',
                '/events/cn/beijing/month/2017-12-01','/events/cn/beijing/month/2017-12-01']
        url = 'https://www.residentadvisor.net/events/cn/beijing/month/2017-12-01'
        request = scrapy.Request(url=url, callback=self.parse)
        yield request


    def parse(self, response):
        events = response.xpath('//a[contains(@itemprop, "url")]/@href').extract()
        for event in events:
            request = scrapy.Request(url= self.domain + event,callback=self.parse_event)
            yield request


    def parse_event(self,response):
        item = ResidentscrapeItem()
        item['eventname'] = response.css('div#sectionHead').xpath('.//h1/text()').extract()[0]
        details = response.css('aside#detail').xpath('.//li')
        for detail in details:
            header = detail.xpath('.//div/text()').extract()[0]
            if 'Date' in header:
                list = detail.xpath('.//text()').extract()
                print(list)
                if len(list) == 4:
                    item['eventdate'] = list[1] + ', '+list[2]
                    item['eventtime'] = list[3]
                continue
        yield item
