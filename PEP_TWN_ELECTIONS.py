# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
from scrapy import Spider
import re, html, os, itertools
from datetime import datetime
from compliance_crawlers.items import PEPItem
# from opencc import OpenCC
from scrapy.exceptions import CloseSpider
import json
from compliance_crawlers.settings import LOG_FILE_PATH
from compliance_crawlers.settings import MONGO_URI
from compliance_crawlers.settings import MONGO_DB
import string
from pymongo import MongoClient
import pymongo
from bs4 import BeautifulSoup
from compliance_crawlers.utils.utils import removePunctuation, remove_space, removeTextInsideParentheses
from compliance_crawlers.utils.table import html_table_to_list

#url: https://db.cec.gov.tw/histMain.jsp?voteSel=20200101A1

class PepTaiwanElections(scrapy.Spider):
    name = 'PEP_TWN_ELECTIONS'
    type = 'pep'
    start_urls = ['https://db.cec.gov.tw/histMain.jsp?voteSel=20200101A1']
    visited = set()

    custom_settings = {
        "LOG_FILE": os.path.join(LOG_FILE_PATH, '{:%Y%m%d%H%M%S}_{}.log'.format(datetime.now(), name)),
        "ROBOTSTXT_OBEY": False
    }

    def replace_year(self,designation):
        pattern = '([0-9]{2,3})年'
        match = re.search(pattern, designation)
        if match:
            tw_year = int(match.group(1))
            year = tw_year+1911
            return re.sub(pattern, f'{str(year)}年', designation)

        return designation

    def create_pep_item(self, name, designation, incumbent, dob, gender, url):
        pepitem = PEPItem()
        name = re.sub(r'\\u[a-z0-9]{4}', '', name)
        key = removePunctuation(name).lower()
        cn_en_name_pattern = r'([^A-Za-z]+)([a-zA-Z].+)'
        match = re.match(cn_en_name_pattern, name)
        if match is not None:
            cn_name = match.group(1).strip()
            en_name = match.group(2).strip()
            en_key = removePunctuation(en_name).lower()
            cn_key = removePunctuation(cn_name)
            pepitem['primary_key'] = cn_key
            pepitem['matching_keywords'] = [cn_key, en_key]
            pepitem['additional_info'] = {
                'name': cn_name,
                'designation': designation,
                'incumbent': incumbent,
                'personal_info': {
                    'en_name': en_name,
                    'dob': dob,
                    'gender': gender
                }
            }
        else:
            pepitem['primary_key'] = key
            pepitem['matching_keywords'] = [key]
            pepitem['additional_info'] = {
            'name': name,
            'designation': designation,
            'incumbent': incumbent,
            'personal_info': {
                'dob': dob,
                'gender': gender,
            }
        }

        pepitem['data_source'] = self.name
        pepitem['region'] = 'TWN'
        pepitem['incumbent'] = incumbent

        return pepitem

    def parse_item(self, response):
        container = response.css('div.payload ul.datasel > li:nth-child(2)')
        for link in container.css('a'):
            if link.css('::text').get().strip() != '不分區政黨':
                path = link.css('::attr(href)').get()
                url = response.urljoin(path)
                yield scrapy.Request(url, dont_filter=True, callback=self.parse_table, meta=response.meta)

    def get_col_number(self, response, name):
        headers = response.css('tr.title > td')
        for i,header in enumerate(headers):
            if header.css('::text').get().strip() == name:
                return i
        return None

    def parse_table(self, response):
        self.visited.add(response.url)
        name_col_number = self.get_col_number(response, '姓名')
        table = response.css('table')
        if name_col_number is None:
            self.logger.info('should not have info ' +response.url)
            for path in response.css('tr.data a::attr(href)').getall():
                depth= 0
                if 'depth' in response.meta:
                    depth = response.meta['depth']
                if depth > 5:
                    self.logger.error(response.url)
                    continue
                url = response.urljoin(path)
                if url in self.visited:
                    continue
                self.visited.add(url)
                yield scrapy.Request(
                    url,
                    callback=self.parse_table,
                    dont_filter=True,
                    meta={"depth": depth+1, 'incumbent': response.meta['incumbent']}
                )
        else:
            data = html_table_to_list(table)
            if len(data) == 0:
                raise Exception('empty table')
            headers = data[0]
            name_col_number = headers.index('姓名')
            gender_col_number = headers.index('性別')
            area_col_number = headers.index('地區')
            dob_col_number = headers.index('出生年次')
            register_col_number = headers.index('當選註記')
            if -1 in {name_col_number, gender_col_number, area_col_number, dob_col_number, register_col_number}:
                raise Exception('missing data')
            designation = response.css('div.titlebox > div > div.head::text').get().split('選舉')[0]

            for row in data[1:]:
                if row[register_col_number] is None or row[register_col_number].strip() != '*':
                    continue
                name = row[name_col_number]
                gender = row[gender_col_number]
                dob = row[dob_col_number]
                area = row[area_col_number]
                yield self.create_pep_item(name, area + self.replace_year(designation), response.meta['incumbent'], dob, gender,response.url)

    def parse(self, response, **kwargs):
        containers = response.css('#history> li')
        for container in containers:
            name = container.css('::text').get().strip()
            if name not in ['國大代表','臺灣省長','臺灣省議員','總統副總統']: # 總統副總統 is scraped in other source already (Taiwan Presidency)
                for i, path in enumerate(container.css('ul > li > a::attr(href)').getall()):
                    meta = {'incumbent': False}
                    if i == 0:
                        meta = {'incumbent': True}
                    url = response.urljoin(path)
                    yield scrapy.Request(url, callback=self.parse_item, dont_filter=True, meta=meta)
