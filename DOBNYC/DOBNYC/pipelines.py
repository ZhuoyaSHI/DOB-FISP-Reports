# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline
from scrapy_redis import connection
from scrapy_redis.scheduler import Scheduler
from urllib.parse import urlparse
import json, os
import csv
import scrapy
from scrapy_redis.connection import get_redis_from_settings
from scrapy.exceptions import IgnoreRequest
from scrapy_redis.pipelines import RedisPipeline
import hashlib

class CustomRedisPipeline(RedisPipeline):
    """
    This pipeline class saves scraped items to Redis and marks their status as 'processing'.

    Method:
        process_item(item, spider): Saves item to Redis and updates its status.
    """

    def process_item(self, item, spider):
        """
        Save item to Redis and mark its status as 'processing'.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider object.

        Returns:
            item (dict): The scraped item.
        """
        super(CustomRedisPipeline, self).process_item(item, spider)

        if item:
            # Convert item to JSON string
            item_str = json.dumps(dict(item), ensure_ascii=False, sort_keys=True)

            # Construct status key
            status_key = f"{spider.name}:items:status"

            # Set item status to 'processing'
            self.server.hset(status_key, item_str, 'processing')

        return item

class FinalizeStatusPipeline:
    """
    This pipeline class marks the status of processed items as 'completed'.

    Method:
        process_item(item, spider): Updates item status to 'completed'.
    """

    def process_item(self, item, spider):
        """
        Update item status to 'completed'.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider object.

        Returns:
            item (dict): The scraped item.
        """
        if hasattr(spider, 'server'):
            # Convert item to JSON string
            item_str = json.dumps(dict(item), ensure_ascii=False, sort_keys=True)

            # Construct status key
            status_key = f"{spider.name}:items:status"

            # Set item status to 'completed'
            spider.server.hset(status_key, item_str, 'completed')

        return item

class DobnycPipeline(object):
    """
    This pipeline class handles scraped items related to real estate property details (FISP) and photographs.

    Methods:
        __init__(self): Initializes the pipeline with required directories and CSV files for error handling.
        init_csv_file(self, filename, fieldnames): Creates a CSV file with specified headers if it doesn't exist.
        write_to_csv(self, filename, data_dict): Writes a dictionary containing data to a specified CSV file.
        process_item(self, item, spider): Processes scraped items, saving FISP data and photos based on validity criteria.
    """

    def __init__(self):
        """
        Initialize the pipeline with required directories and CSV files.

        - Creates necessary directories for storing FISP data (text and JSON) and detailed photos.
        - Initializes CSV files for recording invalid BINs and those lacking a valid cycle (8 or 9).
        """
        self.dirs = [
            os.path.join('Data', 'FISP', '8'),
            os.path.join('Data', 'FISP', '9'),
            os.path.join('Data', 'FISP_json', '8'),
            os.path.join('Data', 'FISP_json', '9'),
            os.path.join('Data', 'Invalid'),
            # os.path.join('Data', 'Detailed Photos', '8'),
            # os.path.join('Data', 'Detailed Photos', '9')
        ]

        # Create each directory
        for directory in self.dirs:
            os.makedirs(directory, exist_ok=True)

        # Initialize CSV files with headers if they don't exist
        self.invalid_bin_file = os.path.join('Data', 'Invalid', 'invalid_bin.csv')
        self.no_8_9_cycle_file = os.path.join('Data', 'Invalid', 'no_8_9_cycle.csv')

        self.init_csv_file(self.invalid_bin_file, ['BIN'])
        self.init_csv_file(self.no_8_9_cycle_file, ['BIN'])

    def init_csv_file(self, filename, fieldnames):
        """
        Creates a CSV file with specified headers if it doesn't exist.

        Args:
            filename (str): The path to the CSV file.
            fieldnames (list): A list of column names (headers) for the CSV file.
        """
        if not os.path.exists(filename):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

    def write_to_csv(self, filename, data_dict):
        """
        Writes a dictionary containing data to a specified CSV file.

        Args:
            filename (str): The path to the CSV file.
            data_dict (dict): A dictionary containing data to be written to the CSV file.
        """
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data_dict.keys())
            writer.writerow(data_dict)

    def process_item(self, item, spider):
        """
        Processes scraped items, saving FISP data and photos based on validity criteria.

        Args:
            item (scrapy.Item): The scraped item containing data.
            spider (scrapy.Spider): The spider object that scraped the item.

        Returns:
            item (scrapy.Item): The processed item.
        """
        adapter = ItemAdapter(item)
        BIN = adapter.get('BIN')
        cycle = adapter.get('cycle')
        FISP = adapter.get('FISP')
        FISP_json = adapter.get('FISP_json')
        print(cycle)

        if adapter.get('FISP') not in ["No cycle 8/9","Invalid BIN"]:
            # Save file
            filename = f"Data/FISP/{cycle[-5]}/{BIN}_{cycle}_FISP.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(FISP)
            filename_json = f"Data/FISP_json/{cycle[-5]}/{BIN}_{cycle}_FISPjson.txt"
            with open(filename_json, 'w', encoding='utf-8') as f:
                json.dump(FISP_json, f, ensure_ascii=False, indent=4)

         # Write the error BINs to different files
        elif adapter.get('FISP') == "No cycle 8/9":
            self.write_to_csv(self.no_8_9_cycle_file, {'BIN': BIN})

        elif adapter.get('FISP') == "Invalid BIN":
            self.write_to_csv(self.invalid_bin_file, {'BIN': BIN})
        return item

'''.git/
The following class is for scraping photo attachements for submitted reports.
'''
# class PhotosPipeline(FilesPipeline):
#     """
#     This pipeline class extends Scrapy's FilesPipeline to handle downloading and storing photos with additional features.

#     It integrates Redis for tracking download statuses and implements custom file naming conventions.

#     Methods:
#         from_settings(cls, settings): Class method to initialize the pipeline from Scrapy settings.
#         __init__(self, store_uri, download_func=None, settings=None): Initializes the pipeline with required attributes.
#         get_media_requests(self, item, info): Generates download requests for photos based on validity criteria.
#         file_path(self, request, response=None, info=None, *, item=None): Generates filenames for downloaded photos.
#         file_downloaded(self, response, request, info, *, item=None): Handles post-download actions and updates Redis.
#     """
#     @classmethod
#     def from_settings(cls, settings):
#         """
#         Inherits from FilesPipeline's class method for initialization from settings.

#         Args:
#             settings (scrapy.Settings): Scrapy settings object.

#         Returns:
#             PhotosPipeline: An instance of the PhotosPipeline class.
#         """
#         pipeline = super().from_settings(settings)
#         pipeline.redis_conn = connection.from_settings(settings)
#         return pipeline
    
#     def __init__(self, store_uri, download_func=None, settings=None):
#         """
#         Initializes the pipeline with required attributes.

#         Args:
#             store_uri (str): The base URI for storing downloaded files. (Inherits from FilesPipeline)
#             download_func (func): Optional download function. (Inherits from FilesPipeline)
#             settings (scrapy.Settings): Scrapy settings object.
#         """
#         super().__init__(store_uri, download_func, settings)
#         self.redis_conn = None
#         self.files_urls_field = 'fileurl'
#         self.redis_conn = get_redis_from_settings(settings)
#         self.redis_key_prefix = 'photo'
        
#     def get_media_requests(self, item, info):
#         """
#         Generates download requests for photos based on validity criteria and handles retries for failed downloads.

#         Args:
#             item (scrapy.Item): The scraped item containing data.
#             info (scrapy.ItemInformation): Contextual information about the scraping process.

#         Yields:
#             scrapy.Request: A Scrapy request object for downloading a photo.
#         """
#         adapter = ItemAdapter(item)
#         url = adapter.get(self.files_urls_field)
#         if url not in ["No cycle 8/9","Invalid BIN","No Photo"]:
#             bin_id = adapter['BIN']
#             cycle = adapter['cycle']
#             photo_count = adapter['photo_count']
#             file_key = f"{bin_id}:{cycle}:{photo_count}:{url}"
#             hash_key = f"{self.redis_key_prefix}"
            
#             if self.redis_conn.get(f"{bin_id}:{file_key}")!=b"downloaded":
#                 self.redis_conn.hset(hash_key, f"{file_key}", 'not_downloaded')
#                 try:
#                     yield scrapy.Request(url, meta={'item': item, 'file_key': file_key})
#                 except ValueError as e:
#                     with open("failedBIN.txt", "a") as f:
#                         f.write(f"{bin_id}\n")
                
                
#     def file_path(self, request, response=None, info=None, *, item=None):
#         """
#         Generates filenames for downloaded photos based on BIN, cycle, and photo count.

#         Args:
#             request (scrapy.Request): The download request object.
#             info (scrapy.ItemInformation): Contextual information about the scraping process.
#             item (scrapy.Item): The scraped item containing data (optional).

#         Returns:
#             str: The generated filename for the downloaded photo.
#         """
#         item = request.meta['item']
#         adapter = ItemAdapter(item)
#         file_extension = os.path.splitext(urlparse(request.url).path)[1]
#         if adapter.get('photo_count') == 0:
#             filename = f"{adapter['cycle'][-5]}/{adapter['BIN']}_{adapter['cycle']}_DetailedPhoto{file_extension}"
#         else:
#             filename = f"{adapter['cycle'][-5]}/{adapter['BIN']}_{adapter['cycle']}_DetailedPhoto({adapter.get('photo_count')}){file_extension}"
#         print(filename)
        
#         return filename
    
#     def file_downloaded(self, response, request, info, *, item=None):
#         """
#         This method is called after a file download is completed. It updates the Redis
#         database to mark the file as downloaded if the download was successful.

#         Args:
#             response (scrapy.http.Response): The response received from the download request.
#             request (scrapy.http.Request): The original request that initiated the download.
#             info (scrapy.pipelines.media.MediaPipeline.SpiderInfo): Pipeline execution context for the spider.
#             item (scrapy.Item, optional): The item being processed.

#         Returns:
#             str: The file checksum returned by the parent class method.
#         """
#         checksum = super().file_downloaded(response, request, info, item=item)
#         if response.status == 200:
#             hash_key = f"{self.redis_key_prefix}"
#             file_key = request.meta['file_key']
#             self.redis_conn.hset(hash_key, f"{file_key}", 'downloaded')
#         return checksum
