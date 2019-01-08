"""
This script gets 1000 random cat facts from the Cat Fact API
(https://catfact.ninja/), and sorts the facts into a tree-like
structure grouped by hierarchical topics, through a simple
bag-of-words model where words have assigned weights that
correspond to node depth.

(Although the limit is 1000, there are acutally <500 cat facts returned
from the API currently). 

This data is saved into a PostgreSQL database for your 
enjoyment.

As a demonstration of scalability, the requests can be done with
multithreading. So in other use cases, or if the future number of
cat facts becomes much greater than 1000, we can hit the same API 
endpoint multiple times simultaneously to get more facts.
"""

from urllib.request import urlopen # Requests would be better but it's not in standard lib
import queue
import json
import string
import re
from threading import Thread 
import psycopg2 # PostgreSQL driver
import psycopg2.extras

API_ENDPOINT = "https://catfact.ninja/facts?limit=1000" 
MAX_SIZE = 1 # This is the max number of tasks that can be put in the multithreaded queue.
             # Since we can get 1000 cat facts at once and there's less than 500 in the API
             # database, we only need to make one request.
REQ_THREAD_COUNT = 1 # Max number of threads to spawn to run queue. Because of the above ^ 
                     # explanation, this also only needs to be set to 1 right now.

DB_NAME = 'catfacts'
DB_TABLE_NAME = "defaulttable"
DB_USER = 'johnny'
DB_HOST = 'localhost'

"""
Below, we will store words (topics) with associated values that 
correspond to how general / encompassing the topic is. Using this 
for the the basis of a weighted bag-of-words model, we can build tree
data structures to group cat facts by hierarchy. This model will be
naive--if any topic is found with a lower (which would be a greater)
weight value than that of the previously found topics in the fact,
the node depth and hierarchy name will be replaced by the topic with
greater weight.

To be clear, weights here are equivalent to node depth, so lower == greater.
If the topic weight val is 0, we build a new tree with this as root node

Matches don't necessarily have to be true synonyms, but words that 
would fall under the topic with the same level of relation (node depth).
Matches also can be repeated, since topics with greater weight will 
have precedent.

To keep things simpler, information about cat breeds / types are completely
ignored, rather than giving each cat breed its own hierarchy of facts. 

Topic words are normalized before checking within the Cat_Facts_Tree class,
to remove punctuation, spaces, capitalization, etc. In the future, we should
apply stemming / lemmatization functions instead of having to repeat plurals and
variations of word forms.
"""
cat_matches = ['animal', 'animals', 'pet', 'pets', 'feline', 'felines', 'cats', 'kitten',
                'kittens', 'kitty']
person_matches = ['people', 'human', 'humans', 'persons', 'caretakers', 'caretaker',
                 'owner', 'owners']
appearance_matches = ["cute", "look", "color", "hair", "adorable", "small", "big", "large", "size",
                     "fat", "skinny", "strong", "robust", "muscular", "muscle", "hard", "soft",
                     "breed", "type", "bred", "grown", "grow", "head", "breeds", "largest",
                     "smallest", "larger", "smaller", "biggest", "smallest", "bigger", "smaller",
                     "softest", "softer", "hariest", "harier", "fluffiest", "fluffier", "hardest", 
                     "harder", "paw", "paws", "feet", "foot", "arm", "tail", "tails", "claw", "claws",
                      "fluff", "fur", "fingers", "finger", "toes", "toe", "nails", "nail"]
personality_matches = ["cute", "look", "color", "adorable", "small", "big", "large", "size",
                    "fat", "skinny", "strong", "robust", "muscular", "muscle", "hard", "soft"]
intelligence_matches =  ["cute", "look", "color", "adorable", "small", "big", "large", "size",
                        "fat", "skinny", "strong", "robust", "muscular", "muscle", "hard", "soft"]
health_matches = ["cute", "look", "color", "hair", "adorable", "small", "big", "large", "size",
                "fat", "skinny", "strong", "robust", "muscular", "muscle", "hard", "soft",
                "life", "live", "expectancy", 'normal']
activities_matches = ["run", "running", "play", "playing", "walk", "walking", "stalk", "stalking",
                      "ran", "played", "walked", "stalked", "talking", "talked", "talks", "runs", 
                      "plays", "walks", "stalks", "hunts", "hunted", "hunting", "hunt", "catch",
                      "caught", "catching", "catches", "bites", "bit", "bite", "prey", "preyed",
                      "preying", "preys", "meows", "meowing", "meowed", "meow", "cries", "crying",
                      "cried", "cry", "yell", "yells", "yelled", "yell", "yawn", "yawns", "yawn",
                      "yawning", "kill", "kills", "killing", "killed", "yelling", "yawning"]
positive_matches = ['good', 'great', 'best', 'fantastic', 'incredible', 'wonderful', 'amazing',
                    'powerful', 'smart', 'intelligent', 'better', 'healthy', 'beautiful', 
                    'super', 'superb', 'awesome', 'love', 'loving', 'fastest', 'fast',
                    'faster']
negative_matches = ['bad', 'lame', 'worst', 'worse', 'stupid', 'dumb', 'pointless', 'idiotic',
                    'moronic', 'weird', 'odd', 'goofy', 'terrible', 'awful', 'unhealthy',
                    'ugly', 'unhealthy', 'hate', 'hateful', 'slowest', 'slower', 'slow']

weighted_topic_vals = {
    "cat": {
        "weight": 1,
        "matches": cat_matches,
        "parents": ["cat_root"]
        },
    # Misc is a catch-all root node for facts that aren't sorted elsewhere
    "misc": {
        "weight": 1
        },
    # Because this is cat facts, put things related to people in new tree
    "person": {
        "weight": 1,
        "parents": ["person_root"],
        "matches": person_matches
    },
    "appearance": {
        "weight": 2,
        "parents": ["cat_root", "cat"],
        "matches": appearance_matches
    },
    "personality": {
        "weight": 2,
        "parents": ["cat_root", "cat"],
        "matches": personality_matches
    },
    "intelligence": {
        "weight": 2,
        "parents": ["cat_root", "cat"],
        "matches": intelligence_matches
    },
    "health": {
        "weight": 2,
        "parents": ["cat_root", "cat"],
        "matches": health_matches
    },
    "activites": {
        "weight": 2,
        "parents": ["cat_root", "cat"],
        "matches": activities_matches
    },
    "positive_health": {
        "weight": 3,
        "parents": ["cat_root", "cat", "health"],
        "matches": positive_matches
    },
    "negative_health": {
        "weight": 3,
        "parents": ["cat_root", "cat", "health"],
        "matches": negative_matches
    },
    "positive_intelligence": {
        "weight": 3,
        "parents": ["cat_root", "cat", "intelligence"],
        "matches": positive_matches
    },
    "negative_intelligence": {
        "weight": 3,
        "parents": ["cat_root", "cat", "intelligence"],
        "matches": negative_matches
    },
    "positive_personality": {
        "weight": 3,
        "parents": ["cat_root", "cat", "personality"],
        "matches": positive_matches
    },
    "negative_personality": {
        "weight": 3,
        "parents": ["cat_root", "cat", "personality"],
        "matches": negative_matches
    },
    "positive_activities": {
        "weight": 3,
        "parents": ["cat_root", "cat", "activities"],
        "matches": positive_matches
    },
    "negative_activities": {
        "weight": 3,
        "parents": ["cat_root", "cat", "activities"],
        "matches": negative_matches
    },
}

# Main class
class Cat_Facts_Tree():
    """
    Because we want the records to be a tree structure with multiple
    roots, we will use a dictionary that stores references to multiple
    trees. So technically it could called Cat_Facts_Trees.

    In the future it would be better to create an actual Tree class
    with class methods / properties, and to store the fact hierarchies
    in that, but for now we can make do with a list of dictionaries with 
    properties of name, depth, and parents. 
    """
    def __init__(self):
        self.cftr = Cat_Facts_Tree_Records()
        return

    def save_to_db_clean(self, tree_data: dict):
        """
        Erases existing table and saves the final tree data into the database wrapper.
        """
        self.cftr.save_to_db_clean(tree_data)
        return

    def normalize_text(self, text: str):
        """ 
        Cleans up text (lower-cases and strips punctuation).
        """
        table = str.maketrans({key: None for key in string.punctuation})
        normalized_text = text.translate(table)
        normalized_text = normalized_text.lower().strip()
        normalized_text = normalized_text.replace("’", "")
        normalized_text = normalized_text.replace("'", "")
        normalized_text = normalized_text.replace('"', "")
        normalized_text = normalized_text.replace(")", "")
        normalized_text = normalized_text.replace("(", "")
        normalized_text = normalized_text.replace("“", "")
        # print("Normalized: ", normalized_text)
        return normalized_text

    def determine_facts_hierarchy(self, facts: list, cat_topics_model: dict):
        """
        Classifies a list of texts (facts) through a predefined model,
        resulting in a dictionary of trees with the facts categorized
        hierarchically, with keys correspond to the belonging topic names.
        """
        tree_dicts = {} # Stores trees in dictionary with keys being root node titles
        for fact in facts:
            _fact = self.normalize_text(fact)
            tokens = _fact.replace('-', ' ').split(' ') # Split into words
            found = False # Did we get a match in the bag-of-words
            parents = [] # Keep track of parent nodes as we categorize every fact
            topic = "" # Keep track of node class title / key
            fact_results = {} # Final results of fact after classifying; to save in tree_dicts
                              # Each dictionary in tree_dicts will be its own tree that houses
                              # a hierarchy of facts based on topic name
            # Check to see if any of the fact tokens are found in the weighted topics model
            for token in tokens:
                if not found:
                    for _topic, vals in cat_topics_model.items():
                        # _topic = _topic.replace("misc_", "")
                        root = _topic                        
                        large_int = 100000000
                        greatest_weight = large_int # Lowest == greatest so start at high num
                                                    # instead of 0
                        if _topic == token:
                            found = True
                            topic = _topic
                            greatest_weight = vals['weight']
                            fact_results['topic'] = topic
                            fact_results['depth'] = greatest_weight
                            fact_results['parents'] = [_topic + "_root"] # The parent will be the root node, which is identified with 
                                                                        # _root appended to the topic
                            fact_results['fact'] = fact
                            parents.append(topic) # Append topic to parents since this will be checked in the child nodes
                            # print("Matched fact for category: ",  topic, "("+str(greatest_weight)+")", " - ", token, " as root node ",
                            #         " : ", fact)
                        else:
                            if 'matches' in vals:
                                for match_word in vals['matches']:
                                    if token == match_word and vals['weight'] < greatest_weight: # Replace greatest_weight with new val
                                                                                                 # if lower (rembemer - greatest weight
                                                                                                 # corresponds to node depth)
                                        found = True
                                        topic = _topic
                                        # We're duplicating logic below (it's unnecessary!), but this makes it easy to save keys inside
                                        # the dictionaries in a specific order, so we can read and parse the data easily when printed.

                                        # This model will be kept simple, so sibling nodes will replace each other as 
                                        # their token matches are parsed later in the text / fact.
                                        if 'parents' in vals:
                                            # We're keeping it simple; if there's more than one possible parent classification the 
                                            # node can belong to, just empty it and push the root node and the latest value found.
                                            parents = []
                                            parents.extend(vals['parents'])
                                            greatest_weight = vals['weight']
                                            parents = list(set(parents))
                                            fact_results['topic'] = topic
                                            if greatest_weight == large_int:
                                                greatest_weight = 0 # If no weight is in the topic models, it'll be a root node
                                            fact_results['depth'] = greatest_weight
                                            fact_results['parents'] = parents
                                            fact_results['fact'] = fact
                                            # print("Matched fact for categories: ", parents, "("+str(greatest_weight)+")", " - ", match_word,
                                            #         " : ", fact)
                                        else:
                                            parents = []
                                            parents.append(root)
                                            greatest_weight = vals['weight']
                                            fact_results['topic'] = topic
                                            if greatest_weight == large_int:
                                                greatest_weight = 0 # If no weight is in the topic models, it'll be a root node
                                            fact_results['depth'] = greatest_weight
                                            fact_results['parents'] = parents
                                            fact_results['fact'] = fact
                                            # print("Matched fact for category: ", topic, "("+str(greatest_weight)+")", " - ", match_word,
                                            #         " : ", fact)
                # The below currently wasn't working right, so for now we will just
                # discard misc facts, or facts that weren't found in the topics model
                # if not found:
                #     # Didn't match facts with bag-of-words cat topics model
                #     # So put that fact in the Misc tree just to keep
                #     greatest_weight = vals['weight']
                #     topic = "misc"
                #     # print("\tPutting fact in misc: ", greatest_weight, " : ", fact)
                #     found = True
            if found:
                if topic in tree_dicts:
                    # print("\n\nAppending fact to topic vals: ", topic, " - ", fact_results, vals)
                    vals = tree_dicts[topic]
                    vals.append(fact_results)
                    tree_dicts[topic] = vals
                else:
                    # We are adding a fact to a new topic for the first time
                    # Check to see if that fact has a depth of 1, and if so, we should create
                    # a root node to house the new hierachy.
                    vals = []
                    if fact_results['depth'] is 1:
                        # print("CREATE ROOT NODE: ", fact_results)
                        # The root node acts as a label, so it contains no fact payload / info
                        # Because it is a label, differentiate the node keyby appending '_root',
                        # so we know it's the start of a new hierarchy.
                        _root = {'topic': str(fact_results['topic']),
                                'depth': 0, 'parents': None, 'fact': None}
                        tree_dicts[topic + '_root'] = _root
                    vals.append(fact_results)
                    tree_dicts[topic] = vals
        print("\n\nPrinting final tree results from classification of cat facts..\n\n")
        for each in tree_dicts.keys():
            print("\nNodes for topic: ", each)
            node = tree_dicts[each]
            if len(node) > 1 and type(node) is list:
                for _each in tree_dicts[each]:
                    print("\t", _each)
            else:
                print("\t(ROOT) ", tree_dicts[each])
        return tree_dicts

    def fetch_from_queue(self, queue: object, results: list):
        """
        Makes request to each URL found in queue. 
        """
        while not queue.empty():
            task = queue.get()
            # Putting a try / except here slows the requests down, but 
            # prevents them from being a single point of failure
            try:
                data = json.loads(urlopen(task).read())
                # Now that our API endpoint returns a list of facts,
                # we iterate through that
                for item in data['data']:
                    results.append(item['fact'])
            except Exception as err:
                print(err)
            queue.task_done()
        return results

    def make_queue(self, api_endpoint:str=API_ENDPOINT, max_size:int=MAX_SIZE):
        """
        Creates a queue to hit an API endpoint X amount of times.
        """
        q = queue.Queue(maxsize=max_size)
        # load up the queue with 100 requests to same URL,
        # since the endpoint only returns one random fact
        # at a time. Tthis could easily be extended to
        # make requests to a list of different URLs.
        for i in range(0, max_size):
            q.put(api_endpoint)
        return q

    def make_cat_facts_tree(self):
        """
        Main function to classify cat facts into a hierarchical data structure. 
        Uses multithreading to do tasks in a queue.
        """
        results = [] # Facts can come in in any order
        facts_queue = self.make_queue(API_ENDPOINT, MAX_SIZE)
        print("\nMaking ", MAX_SIZE, " requests to ", API_ENDPOINT+"\n")
        for i in range(REQ_THREAD_COUNT):
            worker = Thread(target=self.fetch_from_queue, args=(facts_queue, results))
            worker.setDaemon(True)  # Setting threads as "daemon" allows main program to 
                                    # exit eventually even if these dont finish 
                                    # correctly. 
                                    # (https://www.shanelynn.ie/using-python-threading-for-multiple-results-queue/)
            print("Starting worker ", i, worker)
            worker.start()
        facts_queue.join()
        print("\n\nFinished getting ", len(results), " results. Now building tree hierarchy using weighted bag-of-words model..\n\n")
        tree_results = self.determine_facts_hierarchy(results, weighted_topic_vals)
        print("\n\nFinished getting tree hierarchy of cat facts. Now saving into db..")
        return tree_results


# Helper class to call PostgreSQL queries
class Cat_Facts_Tree_Records():
    """
    Wraps around PostgreSQL for persistent cat facts storage. Can save,
    fetch, and create new cat facts. 
    """
    def __init__(self):
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST)
        
        self.conn = conn
        return

    def save_to_db_clean(self, data: dict):
        """
        Saves cat facts into db.
        """
        cur = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        try:
            query = "DROP TABLE " + DB_TABLE_NAME
            print("Clearing old records from db: ", DB_NAME, " for user: ", DB_USER, " at host: ",
                    DB_HOST)
            cur.execute(query)
        except Exception as e:
            print(e)
            cur.execute("ROLLBACK")
            pass
        query = """
        CREATE TABLE """ + DB_TABLE_NAME + """ (
            ID int NOT NULL,
            Depth int NOT NULL,
            Topic varchar(255),
            Parents varchar(255),
            Fact text
            );
            """
        print("Creating table: ", DB_TABLE_NAME, " in", DB_NAME,
                " for user: ", DB_USER, " at host: ", DB_HOST)
        cur.execute(query)
        print("\nSaving records into db: ", DB_NAME, " for user: ", DB_USER, " at host: ",
                DB_HOST+"\n")
        index = 0
        for each in data.keys():
            node = data[each]
            if len(node) > 1 and type(node) is list:
                for _each in data[each]:
                    if 'parents' in _each:
                        if _each['parents'] is None:
                            _each['parents'] = "none"
                        else:
                            val = ", ".join(_each['parents'])
                            _each['parents'] = val
                    else:
                        _each['parents'] = "none"
                    cur.execute("INSERT INTO " + DB_TABLE_NAME + " (ID, Depth, Topic, Parents, Fact) VALUES (%s, %s, %s, %s, %s)",
                                (str(index), str(_each['depth']), str(_each['topic']), str(_each['parents']), str(_each['fact'])))
                    index += 1 # PostgreSQL has no auto-increment so make simple IDs here
                    print("\tSaved record: ", _each)
            else:
                # The node is a root node
                node['parents'] = "none"
                cur.execute("INSERT INTO " + DB_TABLE_NAME + " (ID, Depth, Topic, Parents, Fact) VALUES (%s, %s, %s, %s, %s)",
                                (str(index), str(node['depth']), str(node['topic']), str(node['parents']), str(node['fact'])))
                index += 1 # PostgreSQL has no auto-increment so make simple IDs here
                print("\tSaved record as ROOT NODE: ", node)
        print("\n\nTotal number of records saved: ", str(index) + "\n")
        self.conn.commit()
        cur.close()
        return

    def create(self, values: list):
        """
        Creates and savesa new cat fact or list of cat facts in db.

        Required keys: depth, topic, and fact.
        """
        # We get the last record in the table to get the ID for index
        index = 0
        query = "SELECT * FROM " + DB_TABLE_NAME + " ORDER BY ID DESC LIMIT 1"
        cur = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        cur.execute(query)
        row = cur.fetchone()
        index = row[0] + 1
        results = []
        for each in values:
            if 'parents' in each:
                if each['parents'] is None:
                    each['parents'] = "none"
                else:
                    val = ", ".join(each['parents'])
                    eeach['parents'] = val
            else:
                each['parents'] = "none"
            print("\nCreating new cat fact entry at ", index, each)
            cur.execute("INSERT INTO " + DB_TABLE_NAME + " (ID, Depth, Topic, Parents, Fact) VALUES (%s, %s, %s, %s, %s)",
                        (str(index), str(each['depth']), str(each['topic']), str(each['parents']), str(each['fact'])))
        self.conn.commit()
        cur.close()
        return

    def fetch(self, keys: list=None):
        """
        Gets cat facts by topics / keys from db.
        """
        results = {}
        cur = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        if keys is None:
            query = "SELECT * FROM " + DB_TABLE_NAME
            print("\nFetching all data")
            cur.execute(query)
            rows = cur.fetchall()
            results['all'] = rows
        else:
            for each in keys:
                query = "SELECT * FROM " + DB_TABLE_NAME + " WHERE Topic = %s"
                print("\nFetching data for keys: ", str(each))
                cur.execute(query, (each,))
                rows = cur.fetchall()
                results[each] = rows
        cur.close()
        print("Got results: ", len(results))
        return results


if __name__ == '__main__':
    cft = Cat_Facts_Tree()
    cftr = Cat_Facts_Tree_Records()
    tree = cft.make_cat_facts_tree()
    cft.save_to_db_clean(tree)
    # Lines below for testing 
    # print(cftr.fetch(["cat", "person"]))
    # vals = [{"depth": 1, "topic": "cat", "fact": "Cats are amazing!"}]
    # cftr.create(vals)
    # print(cftr.fetch(["cat"]))
    # print(cftr.fetch())
else:
    cft = Cat_Facts_Tree()
    cftr = Cat_Facts_Tree_Records()