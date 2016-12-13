import json
import MySQLdb
import time
import sys

db = ''
start = time.time()
try:
    db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                         user="alex",           # your username
                         passwd="abc123???",        # your password
                         db="reddit",
                         use_unicode=True,
                         charset="utf8",
                         autocommit=0)  # name of the data base
    print('Successfully connected to mysql.')
except MySQLdb.Error as e:
    print(e)
    print("I am unable to connect to the database")

file_directory = "/home/oerlex/Downloads/data"

counter = 0


cursor = db.cursor()

with open(file_directory, 'r') as f:
    a = f.read().splitlines()
    for line in a:
        data = json.loads(line)
        print(counter)
        counter +=1

        cursor.execute('insert into redditPython (id, parent_id, link_id, name, author, body, subreddit_id, subreddit,'
               'score, created_utc) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
               (data['id'], data['parent_id'], data['link_id'],
                data['name'], data['author'], data['body'],
                data['subreddit_id'], data['subreddit'],
                int(data['score']), int(data['created_utc'])))

cursor.close()
db.commit()
db.close()
end = time.time()

print("Time passed: {}".format(end-start))
