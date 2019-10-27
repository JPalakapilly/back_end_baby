from marshmallow import Schema, fields

# marshmallow stuff
class EventSchema(Schema):
    id = fields.Int(required=True)
    name = fields.Str()
    location = fields.Str()

class UserSchema(Schema):
    id = fields.Int(required=True)
    username = fields.Str()
    event = fields.Nested(EventSchema)
    creator = fields.Boolean()

class RestaurantOptionSchema(Schema):
    id = fields.Int(required=True)
    event = fields.Nested(EventSchema)
    yelpID = fields.Str()

class RestaurantVoteSchema(Schema):
    id = fields.Int(required=True)
    user = fields.Nested(UserSchema)
    restaurant_option = fields.Nested(RestaurantOptionSchema)

class TimeOptionSchema(Schema):
    id = fields.Int(required=True)
    event = fields.Nested(EventSchema)
    timestamp = fields.Time()

class TimeVoteSchema(Schema):
    id = fields.Int(required=True)
    user = fields.Nested(UserSchema)
    time_option = fields.Nested(TimeOptionSchema)


#sqlalchemy stuff

from sqlalchemy import Column, Integer, BigInteger, ForeignKey, String, Boolean, DateTime, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from cockroachdb.sqlalchemy import run_transaction

Base = declarative_base()

# The Account class corresponds to the "accounts" database table.
class Event(Base):
    __tablename__ = 'events'
    id = Column(types.BigInteger, primary_key=True)
    name = Column(String(50))
    location = Column(String(50))

    users = relationship("User")
    restaurant_options = relationship("RestaurantOption")

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50))
    event_id = Column(Integer, ForeignKey('events.id'))
    creator = Column(Boolean)

    restaurant_votes = relationship("RestaurantVote")
    time_votes = relationship("TimeVote")

class RestaurantOption(Base):
    __tablename__ = 'restaurant_options'
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey('events.id'))
    yelp_id = Column(String(50))

    restaurant_votes = relationship("RestaurantVote")

class RestaurantVote(Base):
    __tablename__ = 'restaurant_votes'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    option_id = Column(Integer, ForeignKey('restaurant_options.id'))

class TimeOption(Base):
    __tablename__ = 'time_options'
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey('events.id'))
    timestamp = Column(DateTime)

    time_votes = relationship("TimeVote")

class TimeVote(Base):
    __tablename__ = 'time_votes'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    option_id = Column(Integer, ForeignKey('time_options.id'))

# Create an engine to communicate with the database. The
# "cockroachdb://" prefix for the engine URL indicates that we are
# connecting to CockroachDB using the 'cockroachdb' dialect.
# For more information, see
# https://github.com/cockroachdb/cockroachdb-python.
