#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# @Author  : Ann
# @Time    :

import sqlite3
from os import listdir

filePath = '/Users/anyahui/Documents/CourseraDeepLearning/coursera data/'
dirs = listdir(filePath)
count = 0
n = len(dirs)
courseIdName = dict()
for fileName in dirs:
    if fileName == '.DS_Store':
        pass
    else:
        count += 1
        # print 'processing %d/%d...'%(count,len(dirs)-1)
        f_str = fileName.split('_')
        courseName = f_str[0]
        courseId = f_str[1]
        courseIdName[courseId] = courseName


readData = open('/Users/anyahui/Documents/CourseraDeepLearning/forumName.txt', 'r')

conn = sqlite3.connect('/Users/anyahui/Applications/coursera.db')
conn.text_factory = str
c = conn.cursor()

c.execute('DROP TABLE IF EXISTS "forum_new"')
c.execute('''CREATE TABLE "forum_new" ( \
id integer, forumname text, courseid,coursename text, courseraversion integer, downloaded integer, \
dataset text, numthreads integer, numinter integer, \
primary key(id, courseid))''')
#***********************************for users******************************#
for eachline in readData:
    f_str = eachline.split(',')
    courseid,forumid,forumname = f_str[0],f_str[1],f_str[2].split('\n')[0]
    num_thread = c.execute('select count(*) from thread_new where courseid = ? and forumid = ?', (courseid,forumid,)).fetchall()
    coursename = courseIdName[courseid]
    c.execute('insert or IGNORE into forum_new(courseid, coursename, id, forumname,numthreads) VALUES(?,?,?,?,?)',(courseid,coursename,forumid,forumname,num_thread[0][0],))
    c.execute('update forum_new set forumname = ? where forumname like ? or forumname like ? or forumname like ? or forumname like ? or forumname like ?',
    ('Homework','Assignment%','Practice Problem','Lab%','Programming Assignment%','Problem Sets'))
    c.execute('update forum_new set forumname = ? where forumname like ? or forumname like ?', \
    ('Project', 'Project Increment%', 'Optional Project'))
    c.execute('update forum_new set forumname = ? where forumname like ?',
    ('Discussion', 'Open Critique'))
    c.execute('update forum_new set forumname = ? where forumname like ?',
    ('Errata', 'Course Material Errors'))
    c.execute('update forum_new set forumname = ? where forumname like ? or forumname like ?',
    ('Lecture', 'Technical and Course Questions', 'Course Content / Video Lectures'))
    c.execute('update forum_new set forumname = ? where forumname like ? or forumname like ?',
    ('General', 'General Discussion%','General Discussion'))

c.execute('UPDATE user_new set user_title = ? where user_title = ?', ('Instructor', 'INSTRUCTOR'))
c.execute('UPDATE user_new set user_title = ? where user_title = ? or user_title = ?', ('Staff','TEACHING_STAFF','MENTOR'))
c.execute('UPDATE user_new set user_title = ? where user_title = ? or user_title = ?', ('Student','LEARNER','BROWSER'))
readData.close()
conn.commit()
conn.close()


#Attach /Users/anyahui/Applications/coursera.db to /Users/anyahui/Downloads/lib4moocdata-master/coursera/data/nusdata.db
#Attach database '/Users/anyahui/Applications/coursera' as 'courseraOriginal'

# conn = sqlite3.connect('/Users/anyahui/Downloads/lib4moocdata-master/coursera/data/nusdata.db')
# conn.text_factory = str
# c = conn.cursor()
# c.execute('UPDATE user set user_title = ? where user_title = ?', ('Instructor', 'INSTRUCTOR'))
# c.execute('UPDATE user set user_title = ? where user_title = ? or user_title = ?', ('Staff','TEACHING_STAFF','MENTOR'))
# c.execute('UPDATE user set user_title = ? where user_title = ? or user_title = ?', ('Student','LEARNER','BROWSER'))
# conn.commit()
# conn.close()