#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# @Time    : 3/2/2017 2:12 PM
# @Author  : Ann


import sqlite3
import time

conn = sqlite3.connect('/Users/anyahui/Applications/coursera.db')
c = conn.cursor()

#table: threadid: forumId
#insert into table post(forumid) values (select )


c.execute('select distinct id from thread_new')
threads = c.fetchall()
for row in threads:
    # print row[0]
    threadId = row[0]
    c.execute('select forumid from thread_new where id = ?', (threadId,))
    forumids = c.fetchall()
    for col in forumids:
        forumId = col[0]
        #UPDATE Users SET weight = 160, desiredWeight = 145 WHERE id = 1
        c.execute('UPDATE OR IGNORE post_new SET forumid = ? where thread_id = ?', (forumId,threadId))
        c.execute('UPDATE OR IGNORE comment_new SET forumid = ? where thread_id = ?', (forumId,threadId))
conn.commit()
c.execute('''REPLACE into user_new(id, postid, threadid, forumid, courseid)
select user, id, thread_id, forumid, courseid
from post_new
''')
c.execute('SELECT DISTINCT id from user_temp')
users = c.fetchall()
for user in users:
    userId = user[0]
    # print userId
    c.execute('select full_name from user_temp WHERE id = ?', (userId,))
    fullNames = c.fetchall()
    for name in fullNames:
        userName = name[0]
        c.execute('UPDATE comment_new SET user_name = ? WHERE user = ?', (userName, userId))
    c.execute('select full_name, user_profile, user_title from user_temp where id = ?', (userId,))
    name_ = c.fetchall()
    for row in name_:
        fullName, userProfile, userTitle = row[0], row[1], row[2]
        c.execute('UPDATE user_new SET full_name = ? WHERE id = ?', (fullName, userId))
        c.execute('UPDATE user_new SET user_profile=? WHERE  id = ?', (userProfile, userId))
        c.execute('UPDATE user_new SET user_title=? where id = ?', (userTitle, userId))

c.execute('DELETE FROM post_new WHERE forumid ISNULL')
c.execute('DELETE FROM comment_new WHERE forumid ISNULL')
# c.execute('DELETE FROM comment_new WHERE user_name ISNULL')
# c.execute('DELETE FROM user_new WHERE full_name ISNULL')

conn.commit()
conn.close()

