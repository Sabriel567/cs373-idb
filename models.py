from app import db

class Athlete(db.Model):
    __tablename__ = 'athlete'

    def __init__(self, n):
        self.name = n

class Event(db.Model):
    __tablename__ = 'event'

   def __init__(self, n):
       self.name = n

class Country(db.Model):
    __tablename__ = 'country'

   def __init__(self, n):
      self.name = n

class Year(db.Model):
    __tablename__ = 'year'

    def __init__(self, n):
        self.year = n

class Medal(db.Model):
    __tablename__ = 'medal'

    def __init__(self, n):
        self.rank = n
