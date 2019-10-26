from sqlalchemy.orm import sessionmaker
from cockroachdb.sqlalchemy import run_transaction
import random
from . import schema
# import schema
from sqlalchemy import create_engine
import datetime



class Service():

    def __init__(self):
        self.engine = create_engine(
                                    'cockroachdb://super@localhost:26257/what2eat',
                                    connect_args={'sslmode': 'disable'},
                                    echo=True                   # Log SQL queries to stdout
                                )

        schema.Base.metadata.create_all(self.engine)
        self.sessMaker = sessionmaker(bind=self.engine)
        self.sess = self.sessMaker()

    def createEvent(self, eventName, eventDateTime, location):
        UID = random.getrandbits(63)
        run_transaction(self.sessMaker, lambda s: self._createEvent(s, UID, eventName, location))
        run_transaction(self.sessMaker, lambda s: self._addTimeOption(s, UID, eventDateTime, location))
        return UID

    # Creates an event
    def _createEvent(self, sess, id, eventName, location):
        sess.add(schema.Event(id=id, name=eventName, location=location))

    def _addTimeOption(self, sess, id, eventDateTime):
        sess.add(schema.TimeOption(event_id=id, timestamp=eventDateTime))

    # def getEvent(self, eventName):
    #     ret = self.sess.execute("SELECT * FROM events").fetchone()
    #     placeholder = self.sess.execute("SELECT * FROM time_options").fetchall()
    #     print("time_options: ", placeholder)
    #     print("events: ", ret)

    def login(self, username, eventID, creator):
        # Check if user exists
        uid = self.sess.execute("SELECT ID FROM users WHERE name=:name AND eventUID=:eventUID",
                              {"name": username, "eventUID": eventID})
        print(uid)
        # This could be a result != null. Idk what result looks like
        if len(uid) == 0:
            #User doesn't exist, so we add the user to database
            run_transaction(self.sessMaker, lambda s: self._login(username, eventID, creator))
            uid = self.sess.execute("SELECT ID FROM users WHERE name=:name", {"name": username})

        return uid


    def _login(self, username, eventID, creator):
        self.sess.add(schema.User(name=username, eventUID=eventID, creator=creator))
        # Get the autogenerated UID
        result = self.sess.execute("SELECT UID FROM users WHERE name=:name", {"name": username})
        return result

    def addRestaurant(self, eventID, yelpID):
        # exists = self.sess.execute("SELECT yelp_id FROM restaurant_options").fetchall()
        exists = self.sess.execute("SELECT yelp_id FROM restaurant_options WHERE yelp_id=:yelpID and event_id=:eventID",
                                   {"yelpID": yelpID, "eventID": eventID}).fetchall()
        print("exists in add_Restaurant: ", exists)
        if len(exists) == 0:
            print("Len was 0")
            run_transaction(self.sessMaker, lambda s: self._addRestaurant(s, eventID, yelpID))
        else:
            print("restaurant exists")



    def _addRestaurant(self, sess, eventID, yelpID):
        sess.add(schema.RestaurantOption(event_id=eventID, yelp_id=yelpID))
        print("Success")

    def voteRestaurant(self, userID, yelpID):
        restaurants = self.sess.query(schema.RestaurantVote).filter(schema.RestaurantVote.id==userID)
        self.sess.expungeAll(restaurants)
        self.sess.add(schema.RestaurantVote())
    #
    #
    #
    #     sess.execute("DELETE FROM restaurant_votes WHERE user_ID=:UserID", {"UserID":userID})
    #     sess.execute("SELECT id FROM restaurant_options WHERE yelp_id=:name AND eventUID:=eventUID")

serviceObj = Service()
# print(serviceObj.sess)
# serviceObj.getEvent("CalHacks5")
print("Output of addRestaurant: ", serviceObj.addRestaurant(8441023917761369310, "arbitrary_id"))
# print("Output of createEvent2: ",serviceObj.createEvent("CalHacks6", datetime.datetime.now(), "Berkeley"))
# print("Output of getEvent: ", serviceObj.getEvent("CalHacks5"))
# print("Output of createEvent1: ", serviceObj.createEvent("CalHacks5", datetime.datetime.now(), "Berkeley"))
# print("Output of createEvent2: ",serviceObj.createEvent("CalHacks6", datetime.datetime.now(), "Berkeley"))
# print("Output of getEvent: ", serviceObj.getEvent("CalHacks5"))
