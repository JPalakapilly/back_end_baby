from yelpapi import YelpAPI


API_key = "-YFN8NfOzTZmabtQNQW-5i6DJu1Wl5gpPxVpKbLrSgz8xGrjQzKAGp9VcH0aCFygN-b_R-ODR_mPZcSDSMbf0dtxeeuoO_w1Ielh5L3SFLZcC3xHvs4YSl_2Hoa0XXYx"
client_ID = "62JkuSlLFZ8csatJYFmB7Q"

yelp_api = YelpAPI(API_key)




def restaurant_data_from_ID(yelp_ID):
    restaurant_data = {}
    yelp_response = yelp_api.business_query(id=yelp_ID)
    restaurant_data["yelpID"] = yelp_ID
    restaurant_data["name"] = yelp_response["name"]
    restaurant_data["rating"] = yelp_response["rating"]
    restaurant_data["price"] = len(yelp_response["price"])
    restaurant_data["phone"] = yelp_response["phone"]
    restaurant_data["categories"] = list(map(lambda x: x["title"], yelp_response["categories"]))
    restaurant_data["city"] = yelp_response["location"]["city"]
    restaurant_data["image_url"] = yelp_response["image_url"]
    restaurant_data["photos"] = yelp_response["photos"]
    return restaurant_data


def search(search_string, location_string, num_responses, sort_by='rating'):
    search_response = yelp_api.search_query(term=search_string, location=location_string, limit=num_responses, sort_by=sort_by)
    restaurant_data_list = []
    # this could be optimized if we only want one photo per business
    for business in search_response["businesses"]:
        restaurant_data_list.append(restaurant_data_from_ID(business["id"]))
    return restaurant_data_list
