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


class TaiwanOfficeOfThePresident(scrapy.Spider):
    name = 'PEP_TWN_PRESIDENCY'
    type = 'pep'
    president = 'https://www.president.gov.tw/Page/40'
    vice_president = 'https://www.president.gov.tw/Page/581'
    former_president = 'https://www.president.gov.tw/Page/81'
    former_vice_president = 'https://www.president.gov.tw/Page/82'
    secretary_vice_secretary = 'https://www.president.gov.tw/Page/107'
    minister_advisor = 'https://www.president.gov.tw/Page/109'
    base_url = 'https://www.president.gov.tw'
    custom_settings = {
        "LOG_FILE": os.path.join(LOG_FILE_PATH, '{:%Y%m%d%H%M%S}_{}.log'.format(datetime.now(), name)),
        "ROBOTSTXT_OBEY": False
    }

    def start_requests(self):
        return[scrapy.Request(self.president, callback=self.parse_president),
               scrapy.Request(self.vice_president, callback=self.parse_vice_president),
               scrapy.Request(self.former_president, callback=self.parse_former_president),
               scrapy.Request(self.former_vice_president, callback=self.parse_former_vice_president),
               scrapy.Request(self.secretary_vice_secretary, callback=self.parse_secretaries),
               scrapy.Request(self.minister_advisor, callback=self.parse_senior_advisors)]

    def create_pep_item(self, name, desig, incumbent, nicknames, **kwargs):
        matching_keywords = []
        matching_keywords.append(name)
        desig = '台灣' + desig

        if nicknames is not None:
            for nickname in nicknames:
                matching_keywords.append(nickname)

        pepitem = PEPItem()

        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_PRESIDENCY'
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
            if value:
                personal_info[key] = value
        if len(personal_info.keys()) > 0:
            pepitem['additional_info']['personal_info'] = personal_info

        return pepitem

    def parse_president(self, response):
        name_and_desig = response.xpath("//div[@class='president']//span[@class='h2']//text()").get()
        photo = response.xpath("//div[@class='president_img']//img//@src").get()
        incumbent = True

        name = re.sub('[^\w]|總統', '', name_and_desig).strip()
        desig = re.sub('[^\w]', '', name_and_desig).strip()
        desig = re.sub(name, '', desig).strip()
        photo = self.base_url + photo

        yield self.create_pep_item(name, desig, incumbent, None, photo=photo)

    def parse_vice_president(self, response):
        name_and_desig = response.xpath("//div[@class='president']//span[@class='h2']//text()").get()
        photo = response.xpath("//div[@class='president_img']//img//@src").get()
        incumbent = True

        name = re.sub('[^\w]|副總統', '', name_and_desig).strip()
        desig = re.sub('[^\w]', '', name_and_desig).strip()
        desig = re.sub(name, '', desig).strip()
        photo = self.base_url + photo

        yield self.create_pep_item(name, desig, incumbent, None, photo=photo)

    def parse_former_president(self, response):
        stopwords_list = ['先生', '女士']

        presidents = response.xpath("//div[@class='words']//div[@class='row']")
        desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()
        if desig.find('行憲後歷任') >= 0:
            incumbent = False
        else:
            incumbent = True

        desig = re.sub('行憲後歷任', '', desig).strip()
        stopwords_list.append(desig)

        stopwords = '|'.join(stopwords_list)

        for president in presidents:
            name_and_term = president.xpath(".//p[@class='h4']//text()").get().strip()
            tenure_list = president.xpath(".//span[@class='col-sm-4 col-xs-12 president_img_main']//span//text()").extract()
            photo = president.xpath(".//span[@class='col-sm-4 col-xs-12 president_img_main']//img//@src").get()
            tenure = ''

            name_term_list = name_and_term.split()
            name = name_term_list[1]
            name = re.sub('[^\w]', '', name)
            name = re.sub(stopwords, '', name).strip()
            for years in tenure_list:
                years = re.sub(r'\s+', '', years).strip()
                tenure += years
            photo = self.base_url + photo

            if tenure.find('現任') < 0 and tenure.find('迄今') < 0:
                yield self.create_pep_item(name, desig, incumbent, None, photo=photo, tenure=tenure)

    def parse_former_vice_president(self, response):
        stopwords_list = ['先生', '女士']

        vice_presidents = response.xpath("//div[@class='president_main']//div[@class='row']")
        desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()
        if desig.find('行憲後歷任') >= 0:
            incumbent = False
        else:
            incumbent = True

        desig = re.sub('行憲後歷任', '', desig).strip()
        stopwords_list.append(desig)

        stopwords = '|'.join(stopwords_list)

        for vice_president in vice_presidents:
            name_and_term = vice_president.xpath(".//p[@class='h4']//text()").get().strip()
            tenure_list = vice_president.xpath(".//span[@class='col-sm-4 col-xs-12 president_img_main']//span//text()").extract()
            photo = vice_president.xpath(".//span[@class='col-sm-4 col-xs-12 president_img_main']//img//@src").get()
            tenure = ''

            name_term_list = name_and_term.split()
            name = name_term_list[1]
            name = re.sub('[^\w]', '', name)
            name = re.sub(stopwords, '', name).strip()
            for years in tenure_list:
                years = re.sub(r'\s+', '', years).strip()
                tenure += years
            photo = self.base_url + photo

            if tenure.find('現任') < 0 and tenure.find('迄今') < 0:
                yield self.create_pep_item(name, desig, incumbent, None, photo=photo, tenure=tenure)

    def parse_secretaries(self, response):
        vice_secretary_url = response.xpath("//div[@class='vistit_menu row']//div[@class='togglec']//li[2]/a/@href").get()
        full_vice_secretary_url = f'{self.base_url}{vice_secretary_url}'
        yield scrapy.Request(full_vice_secretary_url, callback=self.parse_vice_secretary)

        stopwords_list = ['先生', '女士']

        name = response.xpath("//div[@class='flag row']//h3//text()").get()
        desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()
        incumbent = True
        photo = response.xpath("//div[@class='col-sm-6 col-xs-12']//img//@src").get()
        education = response.xpath("//ul[@class='words-1'][1]//li//text()").extract()

        stopwords_list.append(desig)
        stopwords = '|'.join(stopwords_list)

        name = re.sub(r'\s+', '', name).strip()
        name = re.sub('[^\w]', '', name)
        name = re.sub(stopwords, '', name)
        desig = '總統府' + desig
        photo = self.base_url + photo

        yield self.create_pep_item(name, desig, incumbent, None, photo=photo, education=education)

        former_secretary = response.xpath("//tbody[@class='text-center']//tr")
        secretary_list = []
        term_list = []
        desig_list = []
        incumbent = False

        for secretary in former_secretary:
            name = secretary.xpath("td[@class='col-sm-4']//text()").get()
            term = secretary.xpath("td[@class='col-sm-8']//text()").get()
            desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()

            if name != None:
                name = re.sub('（', '(', name)
                name = re.sub('）', ')', name)
                new_desig = re.findall('\(([^)]+)', name)

                term = re.sub(r'\s+', '', term).strip()

                if len(new_desig) == 1:
                    if new_desig[0].find('兼') >= 0:
                        desig += new_desig[0]
                    else:
                        desig = new_desig[0]

                name = re.sub(r' ?\([^)]+\)', '', name).strip()
                name = re.sub('[^\w]', '', name)
                desig = '總統府' + desig

                if name in secretary_list:
                    i = secretary_list.index(name)
                    term_list[i] += ',' + term
                    if desig_list[i] != desig:
                        secretary_list.append(name)
                        term_list.append(term)
                        desig_list.append(desig)
                else:
                    secretary_list.append(name)
                    term_list.append(term)
                    desig_list.append(desig)

        for i in range(len(secretary_list)):
            if term_list[i].find('迄今') < 0 and term_list[i].find('現任') < 0:
                yield self.create_pep_item(secretary_list[i], desig_list[i], incumbent, None, tenure=term_list[i])

    def parse_vice_secretary(self, response):
        stopwords_list = ['先生', '女士']

        name = response.xpath("//div[@class='flag row']//h3//text()").get()
        desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()
        incumbent = True
        photo = response.xpath("//div[@class='col-sm-6 col-xs-12']//img//@src").get()
        education = response.xpath("//ul[@class='words-1'][1]//li//text()").extract()

        stopwords_list.append(desig)
        stopwords = '|'.join(stopwords_list)

        name = re.sub(r'\s+', '', name).strip()
        name = re.sub('[^\w]', '', name)
        name = re.sub(stopwords, '', name)
        desig = '總統府' + desig
        photo = self.base_url + photo

        yield self.create_pep_item(name, desig, incumbent, None, photo=photo, education=education)

        former_vice_secretary = response.xpath("//tbody[@class='text-center']//tr")

        for vice_secretary in former_vice_secretary:
            name = vice_secretary.xpath("td/font[@class='tab_spec']//text()").get()
            desig = response.xpath("//ul[@class='breadcrumb']//li[4]//text()").get()
            incumbent = False

            if name != None:
                term = vice_secretary.xpath("td[@class='col-sm-3']//text()").extract()[1]
                name = re.sub('（', '(', name)
                name = re.sub('）', ')', name)
                new_desig = re.findall('\(([^)]+)', name)

                term = re.sub(r'\s+', '', term).strip()

                if len(new_desig) == 1:
                    if new_desig[0].find('兼') >= 0:
                        desig += new_desig[0]
                    else:
                        desig = new_desig[0]

                name = re.sub(r' ?\([^)]+\)', '', name).strip()
                name = re.sub('[^\w]', '', name)
                desig = '總統府' + desig

                yield self.create_pep_item(name, desig, incumbent, None, tenure=term)
            else:
                info = vice_secretary.xpath("td[@class='col-sm-3']//div[@class='col-sm-6 col-xs-12']//text()").extract()

                if len(info) > 0:
                    desig = vice_secretary.xpath("td[@class='col-sm-3']//@data-th").get()
                    desig = '總統府' + desig
                    for i in range(0, len(info) - 1, 2):
                        name = info[i]
                        term = info[i+1]

                        name = re.sub('[^\w]', '', name)
                        term = re.sub(r'\s+', '', term).strip()

                        if term.find('迄今') < 0 and term.find('現任') < 0:
                            yield self.create_pep_item(name, desig, incumbent, None, tenure=term)
                else:
                    others = vice_secretary.xpath("td[@class='col-sm-3']")

                    for other in others:
                        info = other.xpath("div[@class='col-sm-6']//text()").extract()
                        desig = other.xpath("@data-th").get()
                        desig = '總統府' + desig

                        for i in range(0, len(info)-1, 2):
                            name = info[i]
                            term = info[i+1]

                            name = re.sub('[^\w]', '', name)
                            term = re.sub(r'\s+', '', term).strip()

                            if term.find('迄今') < 0 and term.find('現任') < 0:
                                yield self.create_pep_item(name, desig, incumbent, None, tenure=term)

    def parse_senior_advisors(self, response):
        other_advisors_url = response.xpath("//div[@class='toggle toggle-border col-md-12 col-sm-12']//div[@class='togglec']//li//@href").extract()

        del other_advisors_url[0]

        for url in other_advisors_url:
            full_url = f'{self.base_url}{url}'
            yield scrapy.Request(full_url, callback=self.parse_other_advisors)

        senior_advisors = response.xpath("//div[@class='visit01']//div[@class='panel panel-default2 col-sm-6']")
        incumbent = True

        for senior_advisor in senior_advisors:
            name = senior_advisor.xpath(".//div[@class='panel-title2']//span[@class='date_color']/span[1]//text()").get()
            desig = response.xpath("//div[@class='words']//h3//text()").get()
            photo = senior_advisor.xpath(".//div[@class='col-sm-12 col-xs-12']//img//@src").get()

            name = re.sub('（', '(', name)
            name = re.sub('）', ')', name)
            aka = re.findall('\(([^)]+)', name)

            if len(aka) > 0:
                akas = []

                for item in aka:
                    akas.append(item)
            else:
                akas = None

            name = re.sub(r' ?\([^)]+\)', '', name).strip()
            name = re.sub('[^\w]', '', name)
            desig = '總統府' + desig

            if photo == None:
                yield self.create_pep_item(name, desig, incumbent, akas)
            else:
                photo = self.base_url + photo
                birthday = None
                education = None
                additional_infos = senior_advisor.xpath(".//div[@class='content2 people_title']//h4")

                for additional_info in additional_infos:
                    if additional_info.xpath(".//text()").get() == '生日：':
                        birthday = additional_info.xpath("./following-sibling::ul[1]//li//text()").get()
                    elif additional_info.xpath(".//text()").get() == '學歷：':
                        education = additional_info.xpath("./following-sibling::ul[1]//li//text()").extract()

                if education != None:
                    for i in range(len(education)):
                        education[i] = education[i].strip()

                if birthday == None and education == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo)
                elif birthday == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, education=education)
                elif education == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, birthday=birthday)
                else:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, birthday=birthday, education=education)

    def parse_other_advisors(self, response):
        advisors = response.xpath("//div[@class='visit01']//div[@class='panel panel-default2 col-sm-6']")
        incumbent = True

        for advisor in advisors:
            name = advisor.xpath(".//div[@class='panel-title2']//span[@class='date_color']/span[1]//text()").get()
            desig = response.xpath("//div[@class='words']//h3//text()").get()
            photo = advisor.xpath(".//div[@class='col-sm-12 col-xs-12']//img//@src").get()

            name = re.sub('（', '(', name)
            name = re.sub('）', ')', name)
            aka = re.findall('\(([^)]+)', name)

            if len(aka) > 0:
                akas = []

                for item in aka:
                    akas.append(item)
            else:
                akas = None

            name = re.sub(r' ?\([^)]+\)', '', name).strip()
            name = re.sub('[^\w]', '', name)
            desig = '總統府' + desig

            if photo == None:
                yield self.create_pep_item(name, desig, incumbent, akas)
            else:
                photo = self.base_url + photo

                birthday = None
                education = None
                additional_infos = advisor.xpath(".//div[@class='content2 people_title']//h4")

                for additional_info in additional_infos:
                    if additional_info.xpath(".//text()").get() == '生日：':
                        birthday = additional_info.xpath("./following-sibling::ul[1]//li//text()").get()
                    elif additional_info.xpath(".//text()").get() == '學歷：':
                        education = additional_info.xpath("./following-sibling::ul[1]//li//text()").extract()
                if education != None:
                    for i in range(len(education)):
                        education[i] = education[i].strip()
                if birthday == None and education == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo)
                elif birthday == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, education=education)
                elif education == None:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, birthday=birthday)
                else:
                    yield self.create_pep_item(name, desig, incumbent, akas, photo=photo, birthday=birthday, education=education)
