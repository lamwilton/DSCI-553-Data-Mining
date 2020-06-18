import task3train
import task3predict


def prediction_item_based(user, business):
    """
    Item-based prediction formula
    :param user: Testing user
    :param business: Testing business
    :return: Predicted rating
    """
    K = 2  # Number of nearest neighbours to use
    sim_business = {1: 0.5, 3: 0.2, 4: 0.3}  # Get all business pairs from model of business of interest
    user_reviews = {1: 3, 3: 5, 4: 2}  # Get all user reviews from reviews of user of interest
    neighbours = set(sim_business.keys()).intersection(set(user_reviews.keys()))  # Get similar businesses which is rated by that user of interest

    # Get a list of ratings and weights sorted by weights
    # eg [(0.692833855070933, 5.0), (0.6305538695530374, 5.0), (0.5706052915642571, 2.0), ...]
    ratings_list = sorted([(sim_business[i], user_reviews[i]) for i in neighbours], key=lambda x: -x[0])
    ratings_sublist = ratings_list[0:K]
    numerator = sum([x * y for x, y in ratings_sublist])
    denominator = sum([x for x, y in ratings_sublist])
    if denominator == 0:
        return 0
    else:
        result = numerator / denominator
    return result


def prediction_user_based(user, business):
    """
    User-based prediction formula
    :param user: Testing user
    :param business: Testing business
    :return: Predicted rating
    """
    K = 3  # Number of nearest neighbours to use
    sim_user = {2: -1, 4: 0, 5: 0.756}  # Get all similar users from model of user of interest
    business_reviews = {2: 2, 4: 4, 5: 1}  # Get reviews of business of interest
    neighbours = set(sim_user.keys()).intersection(set(business_reviews.keys()))  # Get similar users which is rated by that business of interest
    user_avg = {1: 4.67, 2: 2.5, 4: 4, 5: 3.33}  # Dict of user average stars

    # Get a list of ratings and weights sorted by weights
    # eg [(0.692833855070933, 5.0), (0.6305538695530374, 5.0), (0.5706052915642571, 2.0), ...]
    ratings_list = sorted([(sim_user[i], business_reviews[i], user_avg[i]) for i in neighbours], key=lambda x: -x[0])
    ratings_sublist = ratings_list[0:K]
    numerator = sum([x * (y - z) for x, y, z in ratings_sublist])
    denominator = sum([abs(x) for x, y, z in ratings_sublist])

    # If no corated users at all, return average score of user. If user not in model, return the "UNK" default score
    if numerator == 0 or denominator == 0:
        return 0
    else:
        result = 4.67 + numerator / denominator
    return result

# Page 19 of the slides
print("Testing Pearson ======================================================")
data = [{1: 4, 3: 5, 4: 5}, {1: 4, 2: 2, 3: 1}, {1: 3, 3: 2, 4: 4}, {1: 4, 2: 4}, {1: 2, 2: 1, 3: 3, 4: 5}]
print(task3train.pearson_helper(data, 0, 4))

# Page 37 of slides
print("Testing Prediction item based ======================================================")
print(prediction_item_based(0, 0))

# Page 21 of slides
print("Testing Prediction user based ======================================================")
print(prediction_user_based(0, 0))