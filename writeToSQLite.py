#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# @Time    : 11/8/2016 8:12 PM
# @Author  : Ann
# @Site    : 
# @File    : writeToSQLite.py
# @Software: PyCharm

import json
import sqlite3
import yaml
import time
from os import listdir
import os
from collections import defaultdict


def _byteify(data, ignore_dicts = False):
    """
    To convert any decoded JSON object from using unicode strings to UTF-8-encoded byte strings
    :param data: raw data in json files
    :param ignore_dicts:
    :return: byte string data
    """
    if isinstance(data, unicode):
        return data.encode('utf-8')
    if isinstance(data, list):
        return [ _byteify(item, ignore_dicts=True) for item in data ]
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
            }
    return data

def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )


# To write saved json data into SQLite database.
# STEP1: Create tables: user, thread and post.
# STEP2: Get data from files and insert it into target tables.

def createTables(conn, c):
    """
    Create tables: user, thread and post.
    :param conn: connection to target database
    :param c: cursor of the connection
    :return:
    """

    #***********************************for users******************************#
    c.execute('drop table if EXISTS user')
    c.execute('''CREATE TABLE `user` \
    ( \
    'photoUrl' TEXT, \
    `courseId` TEXT, \
    `userId` INTEGER, \
    `id` TEXT, \
    `learnerId` INTEGER, \
    `courseRole` TEXT, \
    `fullName` TEXT, \
    'externalUserId' TEXT \
    ); ''')
    # ***********************************for threads******************************#
    c.execute('drop table if EXISTS thread')
    c.execute('''CREATE TABLE `thread` \
    ('answerBadge' TEXT, \
    'hasResolved' INTEGER, \
    'instReplied' INTEGER, \
    'totalAnswerCount' INTEGER, \
    'isFollowing' INTEGER, \
    'forumId' TEXT, \
    `lastAnsweredAt` INTEGER, \
    `topLevelAnswerCount` INTEGER, \
    `isFlagged` INTEGER, \
    'lastAnsweredBy' INTEGER, \
    'state' TEXT, \
    'followCount' INTEGER, \
    'title' TEXT, \
    'content' TEXT, \
    'viewCount' INTEGER, \
    'sessionId' TEXT, \
    'creatorId' INTEGER, \
    'isUpvoted' INTEGER, \
    'id' TEXT, \
    'courseId' TEXT, \
    'threadId' TEXT, \
    'createdAt' INTEGER, \
    'upvoteCount' INTEGER \
    ); \
    ''')
    # ***********************************for posts******************************#
    c.execute('drop table if EXISTS post')
    c.execute('''CREATE TABLE `post` \
    ('parentForumAnswerId' TEXT, \
    'forumQuestionId' TEXT, \
    'isFlagged' INTEGER, \
    'order' INTEGER, \
    'content' TEXT, \
    'state' BLOB, \
    'childAnswerCount' INTEGER, \
    'creatorId' INTEGER, \
    'isUpvoted' INTEGER, \
    'id' TEXT, \
    'courseId' TEXT, \
    'postId' TEXT, \
    'createdAt' INTEGER, \
    'upvoteCount' INTEGER \
    ); \
    ''')



#***********************************for users******************************#
def getUsers(userFileName, conn, c):
    filedata = open(userFileName,'r')
    count = 0
    cols_in_database = ['photoUrl','courseId','userId','id','learnerId','courseRole','fullName','externalUserId']
    for eachline in filedata:
        data = json.loads(json.dumps(eachline))
        data = json_loads_byteified(data)
        someitem = data.iterkeys()
        columns = list(someitem)
        if len(columns) != len(cols_in_database):
            cols_notin_data = list(set(columns)^set(cols_in_database))
            for i in xrange(len(cols_notin_data)):
                data[cols_notin_data[i]] = ''
            someitem = data.iterkeys()
            columns = list(someitem)
        else:
            pass
        query = 'insert or ignore into user values (?{1})'
        query = query.format(",".join(columns), ",?" * (len(columns) - 1))
        temp = []
        for keys in data.iterkeys():
            temp.append(data[keys])
        values = tuple(temp)
        c.execute(query,values)
        count += 1
    conn.commit()

#***********************************for threads******************************#
def getThread(threadFileName, conn,c ):
    filedata = open(threadFileName,'r')
    count = 0
    cols_in_database = ['answerBadge','hasResolved','instReplied','totalAnswerCount','isFollowing', 'forumId' ,'lastAnsweredAt', 'topLevelAnswerCount', \
    'isFlagged','lastAnsweredBy','state','followCount' ,'title','content','viewCount','sessionId','creatorId','isUpvoted', \
                        'id','courseId','threadId','createdAt','upvoteCount']
    for eachline in filedata:
        data = json.loads(json.dumps(eachline))
        data = json_loads_byteified(data)
        someitem = data.iterkeys()
        columns = list(someitem)
        if len(columns) != len(cols_in_database):
            cols_notin_data = list(set(columns)^set(cols_in_database))
            for i in xrange(len(cols_notin_data)):
                if cols_notin_data[i] == 'courseId':
                    pass
                elif cols_notin_data[i] == 'threadId':
                    pass
                elif cols_notin_data[i] == 'title':
                    pass
                elif cols_notin_data[i] == 'hasResolved':
                    pass
                elif cols_notin_data[i] == 'instReplied':
                    pass
                else:
                    data[cols_notin_data[i]] = ''
            someitem = data.iterkeys()
            columns = list(someitem)
        else:
            pass
        query = 'insert or ignore into thread values (?{1})'
        query = query.format(",".join(cols_in_database),",?" * (len(cols_in_database) - 1))
        temp = []
        for keys in data.iterkeys():
            if keys == 'answerBadge':
                if data['answerBadge'] == {}:
                    temp.append('')
                    temp.append(0)
                    temp.append(0)
                else:
                    temp.append(data['answerBadge']['answerBadge'])
                    if data['answerBadge']['answerBadge'] == 'MENTOR_RESPONDED':
                        temp.append(1)
                        temp.append(1)
                    elif data['answerBadge']['answerBadge'] == 'INSTRUCTOR_RESPONDED':
                        temp.append(1)
                        temp.append(1)
                    elif data['answerBadge']['answerBadge'] == 'STAFF_RESPONDED':
                        temp.append(1)
                        temp.append(1)
                    else:
                        temp.append(0)
                        temp.append(0)
            elif keys == 'content':
                temp.append(data['content']['question'])
                temp.append(data['content']['details']['definition']['value'])
            elif keys == 'isFlagged':
                if data['isFlagged'] == 'false':
                    temp.append(0)
                else:
                    temp.append(1)
            elif keys == 'isFollowing':
                if data['isFollowing'] == 'false':
                    temp.append(0)
                else:
                    temp.append(1)
            elif keys == 'state':
                if data['state'] == {}:
                    temp.append('')
                else:
                    temp.append('edited')
            elif keys == 'id':
                id_str = data[keys]
                userId,courseId,threadId = id_str.split('~')
                new_str = courseId + '~' + threadId
                temp.append(new_str)
                temp.append(courseId)
                temp.append(threadId)
            else:
                temp.append(data[keys])
        values = tuple(temp)
        c.execute(query,values)
        count += 1
    conn.commit()


#***********************************for posts******************************#
def getPost(postFileName, conn, c):
    filedata = open(postFileName,'r')
    count = 0

    cols_in_database = ['parentForumAnswerId','forumQuestionId','isFlagged','order','content','state','childAnswerCount','creatorId','isUpvoted', \
                        'id','courseId','postId','createdAt','upvoteCount']
    for eachline in filedata:
        data = json.loads(json.dumps(eachline))
        data = json_loads_byteified(data)
        someitem = data.iterkeys()
        columns = list(someitem)
        if len(columns) != len(cols_in_database):
            cols_notin_data = list(set(columns)^set(cols_in_database))
            for i in xrange(len(cols_notin_data)):
                if cols_notin_data[i] == 'courseId':
                    pass
                elif cols_notin_data[i] == 'postId':
                    pass
                else:
                    data[cols_notin_data[i]] = ''
            someitem = data.iterkeys()
            columns = list(someitem)
        else:
            pass
        query = 'insert or ignore into post values (?{1})'
        query = query.format(",".join(cols_in_database),",?" * (len(cols_in_database) - 1))
        temp = []
        for keys in data.iterkeys():
            if keys == 'content':
                temp.append(data['content']['definition']['value'])
            elif keys == 'isFlagged':
                if data['isFlagged'] == 'false':
                    temp.append(0)
                else:
                    temp.append(1)
            elif keys == 'isUpvoted':
                if data['isUpvoted'] == 'false':
                    temp.append(0)
                else:
                    temp.append(1)
            elif keys == 'state':
                if data['state'] == {}:
                    temp.append('')
                else:
                    temp.append('edited')
            elif keys == 'id':
                id_str = data[keys]
                userId,courseId,postId = id_str.split('~')
                new_str = courseId + '~' + postId
                temp.append(new_str)
                temp.append(courseId)
                temp.append(postId)
            else:
                temp.append(data[keys])
        values = tuple(temp)
        c.execute(query,values)
        count += 1
    conn.commit()


if __name__ == "__main__":

    with open('config.yml') as f:
        config = yaml.load(f)
    dbPath = config['dbPath']
    filePath = config['filePath']
    f.close()

    conn = sqlite3.connect(dbPath)
    conn.text_factory = str
    c = conn.cursor()
    createTables(conn, c)

    dirs = listdir(filePath)
    count = 0
    for dirName in dirs:
        totalCourse = len(dirs)
        count += 1
        print "Processing %d...Total %d courses." %(count, totalCourse)
        new_path = filePath + dirName + '/'
        files = listdir(new_path)
        userFileName = ''
        threadFileName = ''
        postFileName = ''
        for file in files:
            if file == 'users.json':
                userFileName = new_path + file
                #print userFileName
            elif file == 'threads.json':
                threadFileName = new_path + file
            elif file == 'posts.json':
                postFileName = new_path + file
        getUsers(userFileName, conn, c)
        print "Writing users OK!"
        getThread(threadFileName, conn, c)
        print "Writing threads OK!"
        getPost(postFileName, conn, c)
        print "Writing posts OK!"
    conn.close()

