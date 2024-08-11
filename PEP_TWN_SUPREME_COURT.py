# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
from scrapy import Spider
from scrapy.http import FormRequest
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
from compliance_crawlers.utils.utils import removePunctuation, remove_space
from compliance_crawlers.utils.table import html_table_to_list

class TaiwanNSB(scrapy.Spider):
    name = 'PEP_TWN_SUPREME_COURT'
    type = 'pep'
    start_url_president = 'https://tps.judicial.gov.tw/tw/cp-1199-33551-b7347-011.html'
    start_url_judge = 'https://tps.judicial.gov.tw/tw/cp-912-33563-d0c25-011.html'
    base_url = 'https://tps.judicial.gov.tw/'
    custom_settings = {
        "LOG_FILE": os.path.join(LOG_FILE_PATH, '{:%Y%m%d%H%M%S}_{}.log'.format(datetime.now(), name)),
        "ROBOTSTXT_OBEY": False
    }

    def start_requests(self):
        return [scrapy.Request(self.start_url_president, callback=self.parse_president),
                scrapy.Request(self.start_url_judge, callback=self.parse_judge)]

    def create_pep_item(self, name, desig, incumbent, **kwargs):
        matching_keywords = []
        matching_keywords.append(name)
        desig = '台灣最高法院' + desig

        pepitem = PEPItem()

        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_SUPREME_COURT'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': incumbent
        }
        pepitem['incumbent'] = incumbent

        personal_info = {}
        for key, value in kwargs.items():
            if value is not None:
                personal_info[key] = value
        if len(personal_info.keys()) > 0:
            pepitem['additional_info']['personal_info'] = personal_info

        return pepitem

    def parse_president(self, response):
        pages = response.xpath('//nav[@aria-label="次選單"]/ul/li/a')

        for page in pages:
            if page.xpath('./@title').get() == '歷任院長':
                url = page.xpath('./@href').get()
                full_url = f'{self.base_url}{url}'
                yield scrapy.Request(full_url, callback=self.parse_past_president)

        stopwords_list = ['先生', '女士', '院長']
        stopwords = '|'.join(stopwords_list)

        name = response.xpath('//section[@class="cp"]/h2/strong/text()').get()
        desig = '院長'
        incumbent = True
        education = []
        infos = response.xpath('//section[@class="cp"]/p/strong//text()').extract()

        name = re.sub(stopwords, '', name).strip()
        name = re.sub('[^\w]', '', name)

        index = -1
        for i in range(len(infos)):
            if infos[i].find('學歷') >= 0:
                index = i

        if index >= 0:
            education.append(infos[index])

            for i in range(index+1, len(infos)):
                if infos[i].find('：') < 0 and infos[i].find(':') < 0:
                    education.append(infos[i])
                else:
                    break

            for i in range(len(education)):
                education[i] = re.sub('學歷', '', education[i])
                education[i] = re.sub('[^\w]', '', education[i])
        else:
            education = None

        yield self.create_pep_item(name, desig, incumbent, education=education)

    def parse_past_president(self, response):
        stopwords_list = ['先生', '女士', '院長']
        stopwords = '|'.join(stopwords_list)

        rows = response.xpath('//table/tbody/tr')

        desig = '院長'
        incumbent = False

        for row in rows:
            name = row.xpath('./td[1]/text()').get()
            tenure = row.xpath('./td[2]/text()').get()

            name = re.sub(stopwords, '', name).strip()
            name = re.sub('[^\w]', '', name)

            if tenure.find('現任') < 0 and tenure.find('迄今') < 0:
                yield self.create_pep_item(name, desig, incumbent)

    def parse_judge(self, response):
        tables = response.xpath('//table')

        for table in tables:
            rows = table.xpath('./tbody/tr')
            desig_broad = table.xpath('./caption/text()').get()

            for row in rows:
                people_list_lines = row.xpath('./td[2]/p/text()').extract()
                desig_specific_list = row.xpath('./td[1]/text()').extract()
                desig_specific = ''
                incumbent = True

                for line in desig_specific_list:
                    desig_specific += line

                desig_specific = re.sub('[^\w]', '', desig_specific)

                desig = desig_broad + desig_specific

                for line in people_list_lines:
                    people_list = re.split('\u3000|\xa0', line)

                    i = 0
                    while i < len(people_list):
                        if people_list[i] == '':
                            del people_list[i]
                        else:
                            i += 1

                    for name in people_list:
                        name = re.sub(r' ?\([^)]+\)', '', name)
                        name = re.sub('[^\w]', '', name)

                        yield self.create_pep_item(name, desig, incumbent)
