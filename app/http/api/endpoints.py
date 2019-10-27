from flask import Flask, json, g, request
from app.event.service import Service
from flask_cors import CORS
import app.yelp.interface as yelp
from collections import OrderedDict
import datetime

app = Flask(__name__)
CORS(app)

service = Service()

@app.route("/search_restaurant", methods=["GET"])
def restaurant_search():
    search_string = request.args["search_string"]
    location_string = request.args["location_string"]
    num_responses=10
    yelp_responses =  yelp.search(search_string, location_string, num_responses, sort_by='rating')
    return json_response({"restaurants": yelp_responses})


@app.route("/login", methods=["POST"])
def login():
    request_data = request.get_json()
    payload = {}
    event_id = request_data["eventID"]
    username = request_data["username"]
    userID = service.login(username, event_id, False)
    payload["user_id"] = userID

    #expect dictionary as response from getRestaurantVotes
    restaurant_votes = service.getVotedRestaurants(event_id, userID)
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
    userID = request_data["userID"]
    YelpID = request_data["YelpID"]
    eventID = request_data["eventID"]
    service.voteRestaurant(userID, yelpID, eventID)
    return json_response({})

@app.route("/get_restaurants", methods=["GET"])
def get_restaurants():
    eventID = request.args["eventID"]
    results = OrderedDict(service.getResults(eventID))
    restaurants = []
    for yelp_id in results:
        restaurants.append(yelp.restaurant_data_from_ID(yelp_id))
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
