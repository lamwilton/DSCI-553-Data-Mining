from collections import defaultdict
import tweepy
import random


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        global tweet_counter, tweets
        MAX_TWEETS = 100

        # Reading a tweet
        tags = [i['text'] for i in status.entities['hashtags']]
        if len(tags) == 0:
            return
        tweet_counter += 1
        print("The number of tweets with tags from the beginning: " + str(tweet_counter))

        # For first 100 tweets, directly save them
        if len(tweets) < MAX_TWEETS:
            tweets.append(tags)
        else:
            # Decide to keep the tweet or not
            keep = random.randint(0, tweet_counter)
            if keep <= MAX_TWEETS:  # 100/n probability of keeping the new tweet (n = tweet_counter)
                replacing = random.randint(0, MAX_TWEETS)  # Pick a random one to replace
                tweets[replacing] = tags
        freq = count_tags(tweets)
        for i in range(0, min(len(freq), 3)):
            print(str(freq[i][0]) + " : " + str(freq[i][1]))

    def on_error(self, status_code):
        if status_code == 420:
            return False


def count_tags(tweets):
    """
    Count the frequencies of tags
    :param tweets: List of tweets containing list of tags per tweet
    :return: Sorted freq list result_sort
    eg [('Trump', 2), ('AspiringDictator', 1), ('DemPartyOfMarxists', 1), ...]
    """
    result = defaultdict(int)
    for tweet in tweets:
        for tag in tweet:
            result[tag] += 1
    result_sort = sorted(result.items(), key=lambda x: (-x[1], x[0]))
    return result_sort


if __name__ == '__main__':
    # ========================================== Initializing API ==========================================
    consumer_token = "IsJLqWb6wKRjxv5I8Irv72ZWV"
    consumer_secret = "LzyxtNL6cTyGnRFobUEr9Q7cqlvqIPALf5oHHIDBaSCSTA7gH1"
    access_token = "2423408268-B3gyV3GtsfBYximwMdNZEiQeqVxD1DeCtkjZwxx"
    access_secret = "lSoElyG9fYqRgtPSjtrfn4wLsbmdoFwDuMxXtau88569w"
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    tweet_counter = 0
    tweets = []
    # ========================================== Main ==========================================
    myStream.filter(track=['trump'])

    print()

