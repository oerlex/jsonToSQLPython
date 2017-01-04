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
        self.cursor.execute('delete from %s',(tableName,))
        print("Table was flushed")



    def insertPlayers(self):
        start = time.time()
        file_directory = "/home/oerlex/Downloads/players"

        counter = 0

        with open(file_directory, 'r') as f:
            a = f.read().splitlines()
            for line in a:
                data = json.loads(line)
                print(counter)
                counter += 1

                ##Players
                self.cursor.execute(
                    'insert ignore player(registration_id, name, shirt_number, games_played, goals_scored, assists, current_club) values (%s,%s,%s,%s,%s,%s,%s)',
                    (data['registration_id'], data['name'], int(data['shirt_number']),
                    int(data['games_played']), int(data['goals_scored']), int(data['assists']),
                    data['current_club']))

            end = time.time()
        print("Time passed: {}".format(end - start))
        print("Inserting players is done")


    def insertClubs(self):
        start = time.time()
        file_directory = "/home/oerlex/Downloads/clubs"

        counter = 0

        with open(file_directory, 'r') as f:
            a = f.read().splitlines()
            for line in a:
                data = json.loads(line)
                print(counter)
                counter += 1

                ##Clubs
                self.cursor.execute(
                    'insert ignore club(name, manager, won, lost, draw, goals_scored, goals_received) values (%s,%s,%s,%s,%s,%s,%s)',
                    (data['name'], data['manager'], int(data['won']),
                     int(data['lost']), int(data['drawn']), int(data['goals_scored']), int(data['goals_received'])))

            end = time.time()
        print("Time passed: {}".format(end - start))
        print("Inserting clubs is done")


    def insertTitles(self):
        start = time.time()
        file_directory = "/home/oerlex/Downloads/titles"

        counter = 0

        with open(file_directory, 'r') as f:
            a = f.read().splitlines()
            for line in a:
                data = json.loads(line)
                print(counter)
                counter += 1

                ##Clubs
                self.cursor.execute(
                    'insert ignore title(name, year, international_national, winner_club) values (%s,%s,%s,%s)',
                    (data['name'], data['year'], data['international_national'],
                     (data['winner_club'])))

            end = time.time()
        print("Time passed: {}".format(end - start))
        print("Inserting titles is done")
        self.cursor.close()


    def closeConnection(self,cursor,db):
        self.cursor.close()
        self.db.commit()
        self.db.close()
        print("The connection to the database has been close")

a = Main()
a.db = a.connectToDB('139.59.131.10','hatem','20Schnappi14','football_league')

a.cursor = a.db.cursor()
#a.clear('player')
#a.clear('club')
#a.clear('title')
a.insertClubs()
a.db.commit()
a.insertPlayers()
a.insertTitles()
a.closeConnection(a.cursor,a.db)
