from sqlalchemy.orm import sessionmaker
from cockroachdb.sqlalchemy import run_transaction
import random
import time
import schema
from sqlalchemy import create_engine
import datetime



class Service():

    def __init__(self):
        self.engine = create_engine(
                                    'cockroachdb://super:super@gcp-us-west2.what2eat.crdb.io:26257/defaultdb?sslmode=verify-full&sslrootcert=./what2eat-ca.crt',
                                    echo=True                   # Log SQL queries to stdout
                                )

        schema.Base.metadata.create_all(self.engine)
        self.sessMaker = sessionmaker(bind=self.engine)
        self.sess = self.sessMaker()

    def createEvent(self, eventName, eventDateTime, location):
        UID = random.randint(0, 10000000)
        run_transaction(self.sessMaker, lambda s: self._createEvent(s, UID, eventName, location))
        self.sess.commit()
        run_transaction(self.sessMaker, lambda s: self._addTimeOption(s, UID, eventDateTime))
        self.sess.commit()
        return UID

    # Creates an event
    def _createEvent(self, sess, id, eventName, location):
        sess.add(schema.Event(id=id, name=eventName, location=location))

    def _addTimeOption(self, sess, id, eventDateTime):
        sess.add(schema.TimeOption(event_id=id, timestamp=eventDateTime))

    def getEvent(self, eventName):
        ret = self.sess.execute("SELECT * FROM events").fetchone()
        placeholder = self.sess.execute("SELECT * FROM time_options").fetchall()

    def login(self, username, eventID, creator):
        # Check if user exists
        uid = self.sess.execute("SELECT id FROM users WHERE username=:name AND event_id=:eventID",
                              {"name": username, "eventID": eventID}).fetchone()
        # This could be a result != null. Idk what result looks like
        if uid is None:
            #User doesn't exist, so we add the user to database
            uid = random.randint(0, 10000000)
            run_transaction(self.sessMaker, lambda s: self._login(s, username, eventID, creator, uid))
            self.sess.commit()
            uid = self.sess.execute("SELECT id FROM users WHERE username=:name AND event_id=:eventID",
                              {"name": username, "eventID": eventID}).fetchone()

        return uid[0]


    def _login(self, sess, username, eventID, creator, uid):
        sess.add(schema.User(id=uid, username=username, event_id=eventID, creator=creator))

    def addRestaurant(self, eventID, yelpID):
        exists = self.sess.execute("SELECT yelp_id FROM restaurant_options WHERE yelp_id=:yelpID and event_id=:eventID",
                                   {"yelpID": yelpID, "eventID": eventID}).fetchone()
        if exists is None:
            run_transaction(self.sessMaker, lambda s: self._addRestaurant(s, eventID, yelpID))
            self.sess.commit()



    def _addRestaurant(self, sess, eventID, yelpID):
        sess.add(schema.RestaurantOption(event_id=eventID, yelp_id=yelpID))

    def getRestaurantID(self, yelpID, eventID):
        result = self.sess.execute("SELECT id from restaurant_options WHERE yelp_id=:yelpID and event_id=:eventID", {"yelpID": yelpID, "eventID": eventID}).fetchone()
        if result is None:
            return None
        return result[0]

    def voteRestaurant(self, userID, yelpID, eventID):
        #Check if restaurant exists
        restaurantID = self.getRestaurantID(yelpID, eventID)
        restaurants = self.sess.query(schema.RestaurantVote).filter(
            schema.RestaurantVote.user_id == userID).all()
        #Delete previous vote and then add new vote
        run_transaction(self.sessMaker, lambda s: self._deleteExistingRestaurants(restaurants))
        self.sess.commit()
        run_transaction(self.sessMaker, lambda s: self._addVote(s, userID, restaurantID))
        self.sess.commit()


    def _addVote(self, sess, userID, optionID):
        sess.add(schema.RestaurantVote(user_id=userID, option_id=optionID))

    def _deleteExistingRestaurants(self, restaurantOptions):
        for restaurant in restaurantOptions:
            self.sess.delete(restaurant)

    def datetimeToMs(self, dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds() * 1000.0

    def eventInfo(self, eventID):
        result = self.sess.execute("SELECT events.name, time_options.timestamp FROM events, time_options WHERE events.id=:eventID and events.id=time_options.event_id ",
                                   {"eventID": eventID}).fetchone()
        if result is None:
            return None
        return result[0], int(self.datetimeToMs(result[1]))

    def getVotedRestaurants(self, userID, eventID):
        results = self.sess.execute("SELECT ro.yelp_id from restaurant_options as ro, restaurant_votes as rv"
                                   " WHERE rv.user_id=:userID and ro.id=rv.option_id",
                                   {"userID": userID, "eventID": eventID}).fetchall()
        ret = []
        for result in results:
            ret.append(result[0])
        return ret

    def getResults(self, eventID):
        ids = self.sess.execute("SELECT id, yelp_id from restaurant_options where event_id=:eventID", {"eventID": eventID}).fetchall()
        results = []
        for id in ids:
            votes = self.sess.execute("SELECT id from restaurant_votes as rv"
                                       " WHERE option_id=:restID",
                                       {"restID": id[0], "eventID": eventID}).fetchall()
            print("getResults votes: ", votes)
            result = (id[1], len(votes))
            results.append(result)
        results.sort(key = lambda x: x[1])
        return results

    def addCached(self, yelpID, name, rating, price, phone, categories, city, image_url, photo1, photo2, photo3):
        run_transaction(self.sessMaker, lambda s: self._addCached(s, yelpID, name, rating, price, phone, city, image_url, photo1, photo2, photo3))
        self.sess.commit()
        for category in categories:
            run_transaction(self.sessMaker, lambda s: self._addCategories(s, yelpID, category))
            self.sess.commit()

    def setCachedDict(self, result, categories):
        ret = {}
        parsedCategories = []

        for category in categories:
            parsedCategories.append(category[0])

        ret["yelpID"] = result[0]
        ret["name"] = result[1]
        ret["rating"] = result[2]
        ret["price"] = result[3]
        ret["phone"] = result[4]
        ret["city"] = result[5]
        ret["image_url"] = result[6]
        ret["photos"] = [result[7], result[8], result[9]]
        ret["categories"] = parsedCategories

        return ret

    def getCached(self, yelpID):
        result = self.sess.execute("SELECT yelp_id from cached_yelps where yelp_id=:yelpID", {"yelpID": yelpID}).fetchone()
        if result is None:
            return None
        else:
            result = self.sess.execute("SELECT yelp_id, name, rating, price, phone, city, image_url, photo1, photo2, photo3 from cached_yelps where yelp_id=:yelpID",
                                       {"yelpID": yelpID}).fetchone()
            categories = self.sess.execute("SELECT category FROM cached_categories WHERE yelp_id=:yelpID", {"yelpID": yelpID}).fetchall()
            ret = self.setCachedDict(result, categories)

            return ret

    def _addCategories(self, sess, yelpID, category):
        sess.add(schema.CachedCategory(yelp_id = yelpID, category = category))

    def _addCached(self, sess, yelpID, name, rating, price, phone, city, image_url, photo1, photo2, photo3):
        sess.add(schema.CachedYelp(yelp_id=yelpID, name=name, rating=rating, price=price, phone=phone,
                                   city=city, image_url=image_url, photo1=photo1, photo2=photo2, photo3=photo3))
