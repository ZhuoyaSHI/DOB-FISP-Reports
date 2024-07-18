import scrapy
from scrapy_redis.spiders import RedisSpider
from DOBNYC.report_process import getText
from DOBNYC.items import DobnycItem
import pandas as pd
import json
from lxml import etree
from scrapy.http import HtmlResponse
from scrapy import signals
import re
from bs4 import BeautifulSoup, NavigableString
import html

class CrawlfispSpider(RedisSpider):
    name = "crawlFISP"
    redis_key = 'crawlFISP:start_urls'
    # allowed_domains = ["a810-dobnow.nyc.gov"]
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CrawlfispSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_opened(self):
        self.process_unfinished_items()
    
    def spider_closed(self, spider, reason):
        self.process_unfinished_items()
        
        
    def __init__(self, *args, **kwargs):
        # Dynamically define the allowed domains list.
        domain = kwargs.pop('domain', 'a810-dobnow.nyc.gov')
        self.allowed_domains = filter(None, domain.split(','))
        super(CrawlfispSpider, self).__init__(*args, **kwargs)
        

    def make_request_from_data(self, data):
        BIN = data.decode('utf-8')
        url = f'https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalPropertyDetailsGet/2%7C{BIN}'
        yield scrapy.Request(url=url, callback=self.parse_BIN, meta={"BIN": BIN})

    def start_requests(self):
        # 这个方法用于初始化 Redis 队列
        # BINs = pd.read_csv(r'BINs.csv')['BIN'].tolist()
        with open ("failedBIN.txt","r") as f:
            BINs = f.read().split("\n")
        
        for BIN in BINs:
            self.server.lpush(self.redis_key, BIN)
        
        # 让 Redis 爬虫开始工作
        return super(CrawlfispSpider, self).start_requests()


    def parse_BIN(self, response):
        BIN = response.meta["BIN"]
        print(BIN)
        StreetName = response.json().get("PropertyDetails","").get("StreetName","")
        
        payload = {"BIN": BIN,"SearchBy": "2","StreetName": f"{StreetName}","FacadesType": 1}

        if StreetName == "":
            items = DobnycItem()
            items['BIN'] = BIN
            items['cycle'] = "Invalid BIN"
            items['fileurl'] = "Invalid BIN"
            items['FISP'] = "Invalid BIN"
            items['FISP_json'] = "Invalid BIN"
            items['photo_count'] = "Invalid BIN"
            yield items

        else:
            yield scrapy.Request(
                url="https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalSafetyDisplay",
                method='POST',
                headers={
                    # 'cookie': response.headers.getlist('Set-Cookie'),
                    'Content-Type': 'application/json'
                },
                body=json.dumps(payload),
                callback=self.parse_cycle,
                meta={"BIN": BIN}
            )
    
    def parse_cycle(self, response):
        BIN = response.meta["BIN"]
        if json.loads(response.text)["IsSuccess"]:
            ListSafetyDetails = json.loads(response.text)["ListSafetyDetails"]
            count = 0
            for entry in ListSafetyDetails:
                if entry["Tr6ReportNumber"][-5] in ['8','9']:
                    count+=1
                    Tr6ReportNumber = entry['Tr6ReportNumber']
                    Tr6Guid = entry["Tr6Guid"]
                    url = f"https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalTR6Details/{Tr6Guid}"
                    yield scrapy.Request(url=url, callback=self.parse_Reports, meta={"BIN":BIN, "Tr6ReportNumber":Tr6ReportNumber})
            if count == 0:
                items = DobnycItem()
                items['BIN'] = BIN
                items['cycle'] = "No cycle 8/9"
                items['fileurl'] = "No cycle 8/9"
                items['FISP'] = "No cycle 8/9"
                items['FISP_json'] = "No cycle 8/9"
                items['photo_count'] = "No cycle 8/9"
                yield items
        else:
            bin_id = BIN
            with open("failedBIN.txt", "a") as f:
                f.write(f"{bin_id}\n")
    
    def parse_Reports(self, response):
        BIN = response.meta["BIN"]
        Tr6ReportNumber = response.meta["Tr6ReportNumber"]
        content = json.loads(response.text)
        FacadesDocumentList = content["FacadesDocumentList"]
        locationDetails = content["locationDetails"]
        Borough = locationDetails.get('Borough')
        ControlNumber = locationDetails.get('ControlNumber')
        cycle = Tr6ReportNumber[-5:]

        items = DobnycItem()
        filecount = 0

        # Finish getting FISP report
        items['BIN'] = BIN
        items['FISP'] = getText("FISPReports.html",content)
        items['FISP_json'] = content
        items['cycle'] = cycle

        found_detailed_photos = False

        if all([FacadesDocumentList, Borough, ControlNumber, Tr6ReportNumber]):
            Payload_Path = rf"\\PortalDownloadedDocuments\\{Borough}\\{ControlNumber}\\{Tr6ReportNumber}\\Supporting Documents\\"
            for entry in FacadesDocumentList:
                if entry["DocumentTypeName"] == "Detailed Photographs":
                    found_detailed_photos = True
                    payload = {
                    "uploadedPath": entry["DocumentUrl"],
                    "downloadPath": Payload_Path
                    }
                    yield scrapy.Request(
                        url="https://a810-dobnow.nyc.gov/Publish/WrapperServicePP/WrapperService.svc/downloadFromDocumentum",
                        method='POST',
                        headers={
                            'Content-Type': 'application/json'
                        },
                        body=json.dumps(payload),
                        callback=self.parse_photos,
                        meta={"items":items,"photo_count":filecount,"BIN":BIN}
                    )
                    filecount += 1

        if not found_detailed_photos:
            items['fileurl'] = "No Photo"
            items['photo_count'] = "No Photo"
            yield items


    def parse_photos(self,response):
        items = response.meta["items"]
        if json.loads(response.text)["IsSuccess"]:
            fileurl = json.loads(response.text)["downloadPath"]
            items['fileurl'] = fileurl
            items['photo_count'] = response.meta["photo_count"]
            yield items
        else:
            bin_id = response.meta["items"]["BIN"]
            with open("failedBIN.txt", "a") as f:
                f.write(f"{bin_id}\n")

    def process_unfinished_items(self):
        status_key = f"{self.name}:items:status"

        # 获取所有已存储的 items 及其状态
        all_items = self.server.hgetall(status_key)

        for item_str, status in all_items.items():
            if status != b'completed':
                # 如果状态不是 'completed'，重新处理这个 item
                item_dict = json.loads(item_str)
                item = DobnycItem(item_dict)
                yield item
                
                
    # def errback(self, failure):
    #     self.logger.error(f"BIN Request failed:{failure.request.meta.get('BIN')}")

    #     if failure.check(HttpError):
    #         response = failure.value.response

    #     if failure.check(HttpError):
    #         response = failure.value.response
    #         self.logger.error('HttpError on %s', response.url)

    #     elif failure.check(DNSLookupError):
    #         request = failure.request
    #         self.logger.error('DNSLookupError on %s', request.url)

    #     elif failure.check(TimeoutError, TCPTimedOutError):
    #         request = failure.request
    #         self.logger.error('TimeoutError on %s', request.url)

    #     with open ("failedBIN.txt", "a") as f:
    #         f.write(f"{failure.request.meta.get('BIN')}\n")

        # scrapy crawl crawlFISP --nolog
