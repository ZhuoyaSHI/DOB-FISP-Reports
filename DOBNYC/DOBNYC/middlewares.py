# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.exceptions import IgnoreRequest
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
import logging
import os
import time

from scrapy.utils.misc import load_object
from scrapy_redis.scheduler import Scheduler
from scrapy.pipelines.files import FilesPipeline

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy import signals
from fake_useragent import UserAgent

class BINControlMiddleware:
    """
    This middleware class controls Binary (BIN) requests and handles failures.

    Attributes:
        failed_bins (set): A set to store failed BINs.
        logger (logging.Logger): A logger object for error logging.
    """

    def __init__(self):
        """
        Initialize the middleware.

        - Initializes failed_bins set to store encountered failed BINs.
        - Initializes logger object for error logging.
        """
        self.failed_bins = set()
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create an instance of the middleware from the crawler.

        Args:
            crawler (scrapy.crawler.Crawler): The crawler object.

        Returns:
            BINControlMiddleware: An instance of the BINControlMiddleware class.
        """
        return cls()

    def process_request(self, request, spider):
        """
        Process request before it is sent.

        Args:
            request (scrapy.Request): The request object.
            spider (scrapy.Spider): The spider object.

        Raises:
            IgnoreRequest: If the request BIN is found in failed_bins set.
        """
        bin = request.meta.get('BIN')
        if bin and bin in self.failed_bins:
            raise IgnoreRequest(f"Ignoring request for failed BIN: {bin}")

    def process_exception(self, request, exception, spider):
        """
        Process exceptions during request handling.

        Args:
            request (scrapy.Request): The request object.
            exception (Exception): The encountered exception.
            spider (scrapy.Spider): The spider object.
        """
        bin = request.meta.get('BIN')
        if bin:
            self.failed_bins.add(bin)
            self.logger.error(f"BIN Request failed: {bin}")

            # Log specific errors
            if isinstance(exception, HttpError):
                self.logger.error(f'HttpError on {request.url}')
            elif isinstance(exception, DNSLookupError):
                self.logger.error(f'DNSLookupError on {request.url}')
            elif isinstance(exception, ConnectionError):
                self.logger.error(f'ConnectionError on {request.url}')
            elif isinstance(exception, (TimeoutError, TCPTimedOutError)):
                self.logger.error(f'TimeoutError on {request.url}')

            self._write_failed_bin(bin)

    def process_response(self, request, response, spider):
        """
        Process response after it is received.

        Args:
            request (scrapy.Request): The request object.
            response (scrapy.Response): The response object.
            spider (scrapy.Spider): The spider object.

        Returns:
            response (scrapy.Response): The response object.
        """
        if response.status >= 400:
            bin = request.meta.get('BIN')
            if bin:
                self.failed_bins.add(bin)
                self.logger.error(
                    f"Error response for BIN {bin}: {response.status},url:{response.url}")
                print(response.url)
                self._write_failed_bin(bin)
        return response

    def _write_failed_bin(self, bin):
        """
        Write failed BIN to a file named 'failedBIN.txt'.

        Args:
            bin (str): The failed BIN to write.
        """
        with open("failedBIN.txt", "a") as f:
            f.write(f"{bin}\n")


class RandomUserAgent:
    """
    This middleware class generates random User-Agent headers for requests.

    Method:
        process_request(request, spider): Generates a random User-Agent header and sets it in the request headers.
    """
    def process_request(self, request, spider):
        ua =  UserAgent(platforms=['pc']).chrome
        request.headers["User-Agent"] = ua


class DobnycSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class DobnycDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
