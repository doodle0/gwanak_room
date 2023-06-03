import sqlite3

class SQLManager:
    def __init__(self, filename):
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()

    def execute(self, cmd):
        self.cur.execute(cmd)

    def fetchall(self):
        return self.cur.fetchall()
