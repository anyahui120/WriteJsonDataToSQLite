#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# @Author  : Ann
# @Time    : 

import sqlite3
import pycurl
import StringIO
import certifi
import json
import yaml
import time
from os import listdir
from selenium import webdriver

class CourseraForumScraper:

    def __init__(self):
        with open('config.yml') as f:
            config = yaml.load(f)
        self.driver = webdriver.PhantomJS()
        self.email = config['UserName']
        self.password = config['Password']
        self.courses = []

    def login(self):
        self.driver.get('https://www.coursera.org/?authMode=login')
        email_field = self.driver.find_element_by_name("email")
        password_field = self.driver.find_element_by_name("password")
        email_field.send_keys(self.email)
        password_field.send_keys(self.password)
        password_field.submit()

    def get_cookie(self,CourseName):
        new_url = 'https://www.coursera.org/learn/%s/discussions' %CourseName
        print new_url
        # course_discussion_urls.append(new_url)
        self.driver.get(new_url)
        # get current cookie and save as file
        cookie_items = [item["name"] + "=" + item["value"] for item in self.driver.get_cookies()]
        cookie = ';'.join(item for item in cookie_items)
        return cookie


    def get_FORUM_info(self,courseId,cookie):
        ForumUrlAPI = 'https://www.coursera.org/api/onDemandCourseForums.v1?q=course&courseId=%s&limit=500&fields=title,description,parentForumId,order,legacyForumId,forumType,groupForums.v1(title,description,parentForumId,order,forumType)' % courseId

        print ForumUrlAPI
        c = pycurl.Curl()
        b = StringIO.StringIO()
        c.setopt(pycurl.CAINFO, certifi.where())
        c.setopt(c.URL, ForumUrlAPI)
        c.setopt(pycurl.CUSTOMREQUEST, 'GET')
        c.setopt(c.WRITEFUNCTION, b.write)
        c.setopt(pycurl.COOKIE, cookie)
        c.perform()
        code = c.getinfo(pycurl.HTTP_CODE)
        forumIdAndName = {}
        if code == 200:
            json_data = b.getvalue()
            elements = json.loads(json_data)['elements']
            print elements
            for e in range(len(elements)):
                id = elements[e]['id']
                forumName = elements[e]['title']
                forumId = id.split('~')[1]
                forumIdAndName[forumId] = forumName
            return forumIdAndName
        else:
            print "This course's discussion gone!"
            return 0


if __name__ == "__main__":

    scraper = CourseraForumScraper()
    scraper.driver.implicitly_wait(10)
    scraper.login()
    time.sleep(3)

    fileOutPut = open('/Users/anyahui/Documents/CourseraDeepLearning/forumName.txt', 'w+')
    filePath = '/Users/anyahui/Documents/CourseraDeepLearning/coursera data/'
    dirs = listdir(filePath)
    count = 0
    n = len(dirs)
    for fileName in dirs:
        if fileName == '.DS_Store':
            pass
        else:
            count += 1
            print 'processing %d/%d...'%(count,len(dirs)-1)
            f_str = fileName.split('_')
            courseName = f_str[0]
            courseId = f_str[1]
            cookie = scraper.get_cookie(courseName)
            print cookie
            forumIdAndName = scraper.get_FORUM_info(courseId, cookie)
            if forumIdAndName == 0:
                print '%s failed!'%courseName
                continue
            for key in forumIdAndName.keys():
                forumId = key
                forumName = forumIdAndName[forumId]
                new_str = courseId + ',' + forumId + ',' + forumName + '\n'
                fileOutPut.write(new_str.encode('utf-8'))
    fileOutPut.close()


    scraper.driver.close()