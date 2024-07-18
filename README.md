# NYC FISP Crawler

## Description
This project implements a Scrapy spider to crawl and extract Facade Inspection & Safety Program (FISP) data in cycle 8 and 9 from the New York City Department of Buildings website. It utilizes Redis for distributed crawling.

## Features
- Extracts FISP reports and related facade photograph reports for specified Building Identification Numbers (BINs)
- Uses Redis for distributed crawling
- Records missing data and errors during the run

## Requirements
- Python 3.11
- See requirements.txt

## Installation
1. Clone the repository
2. Install the required packages: `pip install -r requirements.txt`
3. Ensure Redis is installed and running on your system. Fill in the REDIS_URL in `settings.py`

## Usage
1. Start the Redis server.
2. Run the spider: scrapy crawl crawlFISP

## Output
The spider yields `DobnycItem` objects containing:
- BIN (Building Identification Number)
- FISP report data
- Cycle information
- Photo URLs and counts

Reports will be saved in `Data` Folder:
- Detailed Photos: Facade detailed photo files under cycle 8 and 9 in sub-folder `8` and `9`
- FISP: FISP report in txt format under cycle 8 and 9 in sub-folder `8` and `9`
- FISP_json: FISP report in json format under cycle 8 and 9 in sub-folder `8` and `9`
- Invalid:
  - invalid_bin.csv: DOB has no data for these BINs
  - no_8_9_cycle.csv: There is no data for cycle8/9 for these BINs

## Error Handling
- Failed requests are logged in `failedBIN.txt`
- Rerun the code with a different Redis database and `failedBIN.txt`:
  - Change the Redis database REDIS_DB in settings.py
  - In spider crawlFISP.py, replace BINs = pd.read_csv(r'BINs.csv')['BIN'].tolist() with failedBIN.txt


