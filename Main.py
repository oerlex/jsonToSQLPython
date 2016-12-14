import json
import MySQLdb
import time
import sys


class Main:
    cursor = None
    db =''

    def connectToDB(self,host, user, passwd, database):
        try:
            db = MySQLdb.connect(host=host,     # your host, usually localhost
                                 user=user,     # your username
                                 passwd=passwd, # your password
                                 db=database,
                                 use_unicode=True,
                                 charset="utf8",
                                 autocommit=0)  # name of the data base
            print('Successfully connected to mysql.')
            return db
        except MySQLdb.Error as e:
            print(e)
            print("Unable to connect to the database")

    def clear(tableName,self):
        self.cursor.execute(
            'delete * from' + tableName
        )
        self.cursor.close()

    def insert(self):
        start = time.time()
        file_directory = "/home/oerlex/Downloads/data"

        counter = 0

        with open(file_directory, 'r') as f:
            a = f.read().splitlines()
            for line in a:
                data = json.loads(line)
                print(counter)
                counter += 1

                ##User
                self.cursor.execute('insert ignore authors(idUser) values ' + '(\'' + (data['author']) + '\')')

                ##Subreddit
                self.cursor.execute(
                    'insert ignore into subreddit (idsubreddit, subreddit) values (%s,%s)',
                    (data['subreddit_id'], data['subreddit']))


                ##Link
                self.cursor.execute(
                    'insert ignore into links (idLink, idSubreddit) values (%s,%s)',
                    (data['link_id'], data['subreddit']))


                ##Comment
                self.cursor.execute(
                    'insert ignore into comments (idComment, idParent, idLink, name, idAuthor, body, score, created_utc) values (%s,%s,%s,%s,%s,%s,%s,%s)',
                    (data['id'], data['parent_id'], data['link_id'],
                     data['name'], data['author'], data['body'],
                     int(data['score']), int(data['created_utc'])))



            end=time.time()
        print("Time passed: {}".format(end - start))
        self.cursor.close()


    def closeConnection(self,cursor,db):
        self.cursor.close()
        self.db.commit()
        self.db.close()
        print("The connection to the database has been close")

a = Main()
a.db = a.connectToDB('localhost','alex','abc123???','reddit')
a.cursor = a.db.cursor()
a.insert()
a.closeConnection(a.cursor,a.db)
