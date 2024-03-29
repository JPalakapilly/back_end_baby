from flask import Flask, json, g, request
from service import Service
import interface as yelp
from flask_cors import CORS
from collections import OrderedDict
import datetime
import os


app = Flask(__name__)
CORS(app)

service = Service()

@app.route("/")
def index():
    return "<h1>Welcome to our server !!</h1>"

@app.route("/search_restaurant", methods=["GET"])
def restaurant_search():
    search_string = request.args["search_string"]
    location_string = request.args["location_string"]
    num_responses=10
    restaurant_data_list = []
    if search_string == "":
        yelp_ids = []
    else:
        yelp_ids =  yelp.search(search_string, location_string, num_responses, sort_by='rating')

    for yelp_id in yelp_ids:
        cached_data = service.getCached(yelp_id)
        if cached_data is not None:
            data = cached_data
            data["price"] = len(data["price"])
        else:
            data = yelp.restaurant_data_from_ID(yelp_id)
            photos = data["photos"]
            photo1 = ""
            photo2 = ""
            photo3 = ""
            if len(photos) > 0:
                photo1 = photos[0]
            if len(photos) > 1:
                photo2 = photos[1]
            if len(photos) > 2:
                photo3 = photos[2]

            service.addCached(yelp_id,
                              data["name"],
                              data["rating"],
                              "$" * data["price"],
                              data["phone"],
                              data["categories"],
                              data["city"],
                              data["image_url"],
                              photo1,
                              photo2,
                              photo3)
        restaurant_data_list.append(data)
    return json_response({"restaurants": restaurant_data_list})


@app.route("/login", methods=["POST"])
def login():
    request_data = request.get_json()
    payload = {}
    event_id = request_data["eventID"]
    username = request_data["username"]
    userID = service.login(username, event_id, False)
    payload["user_id"] = userID

    #expect dictionary as response from getRestaurantVotes
    restaurant_votes = service.getVotedRestaurants(userID, event_id)
    payload["restaurant_votes"] = restaurant_votes
    return json_response(payload)

@app.route("/create_event", methods=["POST"])
def create_event():
    request_data = request.get_json()
    eventName = request_data["eventName"]
    eventDateTime = datetime.datetime.fromtimestamp(int(request_data["eventDateTime"])/1000)
    location = request_data["location"]
    event_id = service.createEvent(eventName, eventDateTime, location)

    return json_response({"eventID":event_id})

@app.route("/vote_restaurant", methods=["POST"])
def vote_restaurant():
    request_data = request.get_json()
    eventID = request_data["eventID"]
    YelpID = request_data["YelpID"]
    userID = request_data["userID"]
    service.voteRestaurant(userID, YelpID, eventID)
    return json_response({})

@app.route("/get_restaurants", methods=["GET"])
def get_restaurants():
    eventID = request.args["eventID"]
    results = OrderedDict(service.getResults(eventID))
    restaurants = []
    for yelp_id in results:
        cached_data = service.getCached(yelp_id)
        if cached_data is not None:
            data = cached_data
            data["price"] = len(data["price"])
        else:
            data = yelp.restaurant_data_from_ID(yelp_id)
            photos = data["photos"]
            photo1 = ""
            photo2 = ""
            photo3 = ""
            if len(photos) > 0:
                photo1 = photos[0]
            if len(photos) > 1:
                photo2 = photos[1]
            if len(photos) > 2:
                photo3 = photos[2]

            service.addCached(yelp_id,
                              data["name"],
                              data["rating"],
                              "$" * data["price"],
                              data["phone"],
                              data["categories"],
                              data["city"],
                              data["image_url"],
                              photo1,
                              photo2,
                              photo3)
        restaurants.append(data)
    return json_response({"restaurants": restaurants})


@app.route("/get_results", methods=["GET"])
def get_results():
    eventID = request.args["eventID"]
    sorted_results = OrderedDict(service.getResults(eventID))
    return json_response(sorted_results)


@app.route("/add_restaurant", methods=["POST"])
def add_restaurant():
    request_data = request.get_json()
    YelpID = request_data["YelpID"]
    eventID = request_data["eventID"]
    service.addRestaurant(eventID, YelpID)
    return json_response({})

@app.route("/event_info", methods=["GET"])
def event_info():
    eventID = request.args["eventID"]
    name, time = service.eventInfo(eventID)
    return json_response({"eventName": name, "eventDateTime": time})



def json_response(payload, status=200):
    return (json.dumps(payload), status, {'content-type': 'application/json'})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5002))
    app.run(threaded=False, port=port)
