"""
FISP (Facade Inspection & Safety Program) Spider for NYC Department of Buildings

This spider crawls and extracts FISP-related data and photos from the NYC DOB website.
It uses Redis for distributed crawling and implements various error handling mechanisms.

Date: July 07, 2024
Version: 1.0
"""

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
    """
    A Redis-based spider for crawling FISP (Facade Inspection & Safety Program) data.

    This spider extends RedisSpider to allow for distributed crawling. It processes
    Building Identification Numbers (BINs) to extract FISP reports and related photographs.

    Attributes:
        name (str): The name of the spider.
        redis_key (str): The Redis key for storing start URLs.
        allowed_domains (list): List of allowed domains for crawling.
    """

    name = "crawlFISP"
    redis_key = 'crawlFISP:start_urls'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        Factory method that creates a new instance of the spider from a crawler.

        Args:
            crawler (scrapy.crawler.Crawler): The crawler object.

        Returns:
            CrawlfispSpider: An instance of the spider.
        """
        spider = super(CrawlfispSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_opened(self):
        """
        Method called when the spider is opened.
        Processes any unfinished items from previous runs.
        """
        self.process_unfinished_items()
    
    def spider_closed(self, spider, reason):
        """
        Method called when the spider is closed.
        Processes any remaining unfinished items.

        Args:
            spider (CrawlfispSpider): The spider instance.
            reason (str): The reason for closing the spider.
        """
        self.process_unfinished_items()
        
    def __init__(self, *args, **kwargs):
        """
        Initialize the spider with dynamic domain settings.

        Args:
            domain (str): Comma-separated list of allowed domains.
        """
        domain = kwargs.pop('domain', 'a810-dobnow.nyc.gov')
        self.allowed_domains = filter(None, domain.split(','))
        super(CrawlfispSpider, self).__init__(*args, **kwargs)

    def make_request_from_data(self, data):
        """
        Create a request from Redis data.

        Args:
            data (bytes): The BIN data from Redis.

        Yields:
            scrapy.Request: A request object for the BIN URL.
        """
        BIN = data.decode('utf-8')
        url = f'https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalPropertyDetailsGet/2%7C{BIN}'
        yield scrapy.Request(url=url, callback=self.parse_BIN, meta={"BIN": BIN})

    def start_requests(self):
        """
        Initialize the Redis queue with BINs from a file.

        Returns:
            Iterator: An iterator of initial requests.
        """
        BINs = pd.read_csv(r'BINs.csv')['BIN'].tolist()

        # Rerun the code after the first run completes. Change redis database before the second run.
        # with open("failedBIN.txt", "r") as f:
        #     BINs = f.read().split("\n")
        
        for BIN in BINs:
            self.server.lpush(self.redis_key, BIN)
        
        return super(CrawlfispSpider, self).start_requests()

    def parse_BIN(self, response):
        """
        Parse the BIN response and generate a request for safety display data.

        Args:
            response (scrapy.http.Response): The response object.

        Yields:
            scrapy.Request or DobnycItem: Either a new request for safety data or an item for invalid BINs.
        """
        BIN = response.meta["BIN"]
        StreetName = response.json().get("PropertyDetails", "").get("StreetName", "")
        
        payload = {"BIN": BIN, "SearchBy": "2", "StreetName": f"{StreetName}", "FacadesType": 1}

        if StreetName == "":
            yield DobnycItem(BIN=BIN, cycle="Invalid BIN", fileurl="Invalid BIN",
                             FISP="Invalid BIN", FISP_json="Invalid BIN", photo_count="Invalid BIN")
        else:
            yield scrapy.Request(
                url="https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalSafetyDisplay",
                method='POST',
                headers={'Content-Type': 'application/json'},
                body=json.dumps(payload),
                callback=self.parse_cycle,
                meta={"BIN": BIN}
            )
    
    def parse_cycle(self, response):
        """
        Parse the safety display response and generate requests for TR6 reports.

        Args:
            response (scrapy.http.Response): The response object.

        Yields:
            scrapy.Request or DobnycItem: Either requests for TR6 reports or items for no cycles.
        """
        BIN = response.meta["BIN"]
        if json.loads(response.text)["IsSuccess"]:
            ListSafetyDetails = json.loads(response.text)["ListSafetyDetails"]
            count = 0
            for entry in ListSafetyDetails:
                if entry["Tr6ReportNumber"][-5] in ['8','9']:
                    count += 1
                    Tr6ReportNumber = entry['Tr6ReportNumber']
                    Tr6Guid = entry["Tr6Guid"]
                    url = f"https://a810-dobnow.nyc.gov/Publish/WrapperPP/PublicPortal.svc/getPublicPortalTR6Details/{Tr6Guid}"
                    yield scrapy.Request(url=url, callback=self.parse_Reports, meta={"BIN":BIN, "Tr6ReportNumber":Tr6ReportNumber})
            if count == 0:
                yield DobnycItem(BIN=BIN, cycle="No cycle 8/9", fileurl="No cycle 8/9",
                                 FISP="No cycle 8/9", FISP_json="No cycle 8/9", photo_count="No cycle 8/9")
        else:
            with open("failedBIN.txt", "a") as f:
                f.write(f"{BIN}\n")
    
    def parse_Reports(self, response):
        """
        Parse TR6 report details and generate requests for photo downloads.

        Args:
            response (scrapy.http.Response): The response object.

        Yields:
            scrapy.Request or DobnycItem: Either requests for photo downloads or items with report data.
        """
        BIN = response.meta["BIN"]
        Tr6ReportNumber = response.meta["Tr6ReportNumber"]
        content = json.loads(response.text)
        FacadesDocumentList = content["FacadesDocumentList"]
        locationDetails = content["locationDetails"]
        Borough = locationDetails.get('Borough')
        ControlNumber = locationDetails.get('ControlNumber')
        cycle = Tr6ReportNumber[-5:]

        items = DobnycItem(
            BIN=BIN,
            FISP=getText("FISPReports.html", content),
            FISP_json=content,
            cycle=cycle
        )

        if all([FacadesDocumentList, Borough, ControlNumber, Tr6ReportNumber]):
            Payload_Path = rf"\\PortalDownloadedDocuments\\{Borough}\\{ControlNumber}\\{Tr6ReportNumber}\\Supporting Documents\\"
            for entry in FacadesDocumentList:
                if entry["DocumentTypeName"] == "Detailed Photographs":
                    payload = {
                        "uploadedPath": entry["DocumentUrl"],
                        "downloadPath": Payload_Path
                    }
                    yield scrapy.Request(
                        url="https://a810-dobnow.nyc.gov/Publish/WrapperServicePP/WrapperService.svc/downloadFromDocumentum",
                        method='POST',
                        headers={'Content-Type': 'application/json'},
                        body=json.dumps(payload),
                        callback=self.parse_photos,
                        meta={"items": items, "photo_count": 0, "BIN": BIN}
                    )
                    return
        
        items['fileurl'] = "No Photo"
        items['photo_count'] = "No Photo"
        yield items

    # def parse_photos(self, response):
    #     """
    #     Parse the photo download response and yield the final item.

    #     Args:
    #         response (scrapy.http.Response): The response object.

    #     Yields:
    #         DobnycItem: The final scraped item with all collected data.
    #     """
    #     items = response.meta["items"]
    #     if json.loads(response.text)["IsSuccess"]:
    #         fileurl = json.loads(response.text)["downloadPath"]
    #         items['fileurl'] = fileurl
    #         items['photo_count'] = response.meta["photo_count"]
    #         yield items
    #     else:
    #         bin_id = response.meta["items"]["BIN"]
    #         with open("failedBIN.txt", "a") as f:
    #             f.write(f"{bin_id}\n")

    def process_unfinished_items(self):
        """
        Process any unfinished items from previous spider runs.

        This method retrieves stored items from Redis that were not fully processed
        in previous runs and yields them for reprocessing.

        Yields:
            DobnycItem: Unfinished items for reprocessing.
        """
        status_key = f"{self.name}:items:status"
        all_items = self.server.hgetall(status_key)

        for item_str, status in all_items.items():
            if status != b'completed':
                item_dict = json.loads(item_str)
                yield DobnycItem(item_dict)

# Command to run the spider: scrapy crawl crawlFISP --nolog