from collections import defaultdict
import tweepy
import random
import sys


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        """
        Reading a tweet suscessfully
        :param status:
        :return:
        """
        global tweet_counter, tags_bin
        MAX_TAGS = 100

        # Reading a tweet
        tweet_tags = [i['text'] for i in status.entities['hashtags']]
        if len(tweet_tags) == 0:
            return
        tweet_counter += 1
        # print("The number of tweets with tags from the beginning: " + str(tweet_counter))

        for tag in tweet_tags:
            # For first 100 tags, directly save them
            if len(tags_bin) < MAX_TAGS:
                tags_bin.append(tag)
            else:
                # Decide to keep the tweet or not
                keep = random.randint(0, tweet_counter - 1)
                if keep <= MAX_TAGS:  # 100/n probability of keeping the new tweet (n = tweet_counter)
                    replacing = random.randint(0, MAX_TAGS - 1)  # Pick a random one to replace
                    tags_bin[replacing] = tag
        freq = count_tags(tags_bin)

        # Write results
        with open(output_file_name, "a+") as file:
            file.write("The number of tweets with tags from the beginning: " + str(tweet_counter))
            file.write("\n")
            for i in range(0, min(len(freq), 3)):
                file.write((freq[i][0]) + " : " + str(freq[i][1]))
                file.write("\n")
            file.write("\n")
        return

    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_exception(self, exception):
        print("Somethings wrong, ignoring it")
        return


def count_tags(tags_bin):
    """
    Count the frequencies of tags
    :param tags_bin: Tags bin
    :return: Sorted freq list result_sort
    eg [('Trump', 2), ('AspiringDictator', 1), ('DemPartyOfMarxists', 1), ...]
    """
    result = defaultdict(int)
    for tag in tags_bin:
        result[tag] += 1
    result_sort = sorted(result.items(), key=lambda x: (-x[1], x[0]))
    return result_sort


if __name__ == '__main__':
    # ========================================== Initializing API ==========================================
    port_num = int(sys.argv[1])
    output_file_name = sys.argv[2]
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
    tags_bin = []
    open(output_file_name, "w")  # Create file if not exist

    # ========================================== Main ==========================================
    myStream.filter(track=['trump'], languages=["en"])


