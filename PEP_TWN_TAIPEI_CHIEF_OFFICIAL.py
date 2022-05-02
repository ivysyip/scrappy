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

#websites urls: https://www.gov.taipei/News.aspx?n=19FA75E3DEDDDA1F&sms=74724DD2D5D1AF52
#               https://www.gov.taipei/News.aspx?n=25377E0A6026BC5C&sms=692DD1623EAE1155
#               https://www.gov.taipei/cp.aspx?n=53286BCFCDEE9493
#               https://www.gov.taipei/News_Content.aspx?n=EE4BCBC62725FD4A&sms=E7B05119A251FAC0&s=44A04DC0691F4B4A
#               https://www.gov.taipei/cp.aspx?n=C1304E1985BE745E

class TaipeiChief(scrapy.Spider):
    name = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
    type = 'pep'
    vice_mayor = 'https://www.gov.taipei/News.aspx?n=19FA75E3DEDDDA1F&sms=74724DD2D5D1AF52'
    vice_secretary = 'https://www.gov.taipei/News.aspx?n=25377E0A6026BC5C&sms=692DD1623EAE1155'
    mayor = 'https://www.gov.taipei/cp.aspx?n=53286BCFCDEE9493'
    secretary = 'https://www.gov.taipei/News_Content.aspx?n=EE4BCBC62725FD4A&sms=E7B05119A251FAC0&s=44A04DC0691F4B4A'
    former_mayors = 'https://www.gov.taipei/cp.aspx?n=C1304E1985BE745E'
    base_url = 'https://www.gov.taipei/'
    custom_settings = {
        "LOG_FILE": os.path.join(LOG_FILE_PATH, '{:%Y%m%d%H%M%S}_{}.log'.format(datetime.now(), name)),
        "ROBOTSTXT_OBEY": False
    }

    def start_requests(self):
        return [scrapy.Request(self.vice_mayor, callback=self.parse_vice_mayors),
                scrapy.Request(self.vice_secretary, callback=self.parse_vice_secretaries),
                scrapy.Request(self.mayor, callback=self.parse_mayor),
                scrapy.Request(self.secretary, callback=self.parse_secretary),
                scrapy.Request(self.former_mayors, callback=self.parse_former_mayors)]

    def parse_vice_mayors(self, response):
        vice_mayor_urls = response.xpath("//td[@class='CCMS_jGridView_td_Class_1']//a//@href").extract()

        for link in vice_mayor_urls:
            full_url = f'{self.base_url}{link}'
            # print(full_url)
            yield scrapy.Request(full_url, callback=self.parse_vice_mayors_detail)

    def parse_vice_mayors_detail(self, response):
        name = response.xpath("//div[@class='cp interduce']//li[1]//text()").extract()[1]
        matching_keywords = []
        matching_keywords.append(name)
        desig = response.xpath("//h2[@class='h3']//span//text()").get()
        photo = response.xpath("//div[@class='cp interduce']//img//@src").get()
        education = response.xpath("//div//ul[@class='cp interduce-list']//li[2]//text()").extract()

        for i in range(len(education)):
            education[i] = re.sub('\t', ' ', education[i]).strip()
        del education[0]
        desig = '台北市' + desig

        pepitem = PEPItem()
        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': True
        }
        pepitem['additional_info']['personal_info'] = {
            'photo': photo,
            'education': education
        }
        pepitem['incumbent'] = True
        yield pepitem

    def parse_vice_secretaries(self, response):
        vice_secretary_urls = response.xpath("//td[@class='CCMS_jGridView_td_Class_1']//a//@href").extract()

        for url in vice_secretary_urls:
            full_url = f'{self.base_url}{url}'
            # print(full_url)
            yield scrapy.Request(full_url, callback=self.parse_vice_secretaries_detail)

    def parse_vice_secretaries_detail(self, response):
        name = response.xpath("//div[@class='cp interduce']//li[1]//text()").extract()[1]
        matching_keywords = []
        matching_keywords.append(name)
        desig = response.xpath("//h2[@class='h3']//span//text()").get()
        photo = response.xpath("//div[@class='cp interduce']//img//@src").get()
        education = response.xpath("//div//ul[@class='cp interduce-list']//li[2]//text()").extract()

        for i in range(len(education)):
            education[i] = re.sub('\t', ' ', education[i]).strip()
        del education[0]
        desig = '台北市' + desig

        pepitem = PEPItem()
        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': True
        }
        pepitem['additional_info']['personal_info'] = {
            'photo': photo,
            'education': education
        }
        pepitem['incumbent'] = True
        yield pepitem

    def parse_mayor(self, response):
        name = response.xpath("//div//ul//span[@class='bigsize']//text()").get()
        desig = response.xpath("//h2[@class='h3']//span//text()").get()
        photo = response.xpath("//div[@class='cp interduce']//img//@src").get()
        education = response.xpath("//div[@class='cp interduce']//ul[@class='cp interduce-list']//li[2]//li//text()").extract()
        matching_keywords = []
        matching_keywords.append(name)

        desig = re.sub('現任', '', desig).strip()
        desig = '台北市' + desig

        pepitem = PEPItem()
        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': True
        }
        pepitem['additional_info']['personal_info'] = {
            'photo': photo,
            'education': education
        }
        pepitem['incumbent'] = True
        yield pepitem

    def parse_secretary(self, response):
        name = response.xpath("//div//ul[@class='cp interduce-list']//li[1]//text()").extract()[1]
        matching_keywords = []
        matching_keywords.append(name)
        desig = response.xpath("//h2[@class='h3']//span//text()").get()
        photo = response.xpath("//div[@class='cp interduce']//img//@src").get()
        education = response.xpath("//div//ul[@class='cp interduce-list']//li[2]//text()").extract()

        for i in range(len(education)):
            education[i] = re.sub('\t', ' ', education[i]).strip()
        del education[0]
        desig = '台北市' + desig

        pepitem = PEPItem()
        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': True
        }
        pepitem['additional_info']['personal_info'] = {
            'photo': photo,
            'education': education
        }
        pepitem['incumbent'] = True
        yield pepitem

    def parse_former_mayors(self, response):
        names = response.xpath("//div[@class='area-table rwd-straight mayor'][3]//tbody//tr//td[@data-title='姓名']//span//text()").extract()
        desig = response.xpath("//h2[@class='h3']//span//text()").get()
        desig = re.sub('歷任', '', desig).strip()
        desig = '台北市' + desig
        photos = response.xpath("//div[@class='area-table rwd-straight mayor'][3]//tbody//tr//td[@data-title='肖像']//img//@src").extract()
        terms = response.xpath("//div[@class='area-table rwd-straight mayor'][3]//tbody//tr//td[@data-title='屆數']//span//text()").extract()
        dates = response.xpath("//div[@class='area-table rwd-straight mayor'][3]//tbody//tr//td[@data-title='任期']//span//text()").extract()

        i = 0
        while i < len(dates):
            dates[i] = re.sub('\t', ' ', dates[i]).strip()
            if (dates[i] == '|'):
                del dates[i]
            else:
                i += 1

        i = 0
        while i < len(dates)-1:
            dates[i] = dates[i] + ' to ' + dates[i+1]
            del dates[i+1]
            i += 1

        i = 0
        while i < len(names) - 1:
            if (names[i] == names[i+1]):
                del names[i+1]
                del photos[i+1]
                terms[i] = terms[i] + '-' + terms[i+1]
                del terms[i+1]
                start_date = dates[i][:dates[i].index('to')]
                end_date = dates[i+1][dates[i+1].index('to'):]
                dates[i] = start_date + end_date
                del dates[i+1]
            else:
                i += 1

        for i in range(len(names)):
            matching_keywords = []
            matching_keywords.append(names[i])

            pepitem = PEPItem()
            pepitem['primary_key'] = names[i]
            pepitem['data_source'] = 'PEP_TWN_TAIPEI_CHIEF_OFFICIAL'
            pepitem['matching_keywords'] = matching_keywords
            pepitem['region'] = 'TWN'
            pepitem['additional_info'] = {
                'name': names[i],
                'designation': desig,
                'incumbent': False
            }
            pepitem['additional_info']['personal_info'] = {
                'photo': photos[i],
                'term': terms[i],
                'term of office': dates[i]
            }
            pepitem['incumbent'] = False
            yield pepitem
