# Rest API for to get / write data for a Cat_Facts_Tree
# Uses db calls in Cat_Facts_Tree_Records 
from flask import Flask, jsonify, request
from cat_facts_tree import Cat_Facts_Tree_Records

cftr = Cat_Facts_Tree_Records()
app = Flask(__name__)

@app.route("/api/get_cat_facts/<string:topics>", methods=['GET'])
def get_cat_facts(topics: str="all"):
    """
    Gets cat facts by topical category or by multiple topics. 

    It accepts a string parameter but can parse topics by commas (",").

    To get all cat facts, just request with "all".

    For a full list of topics, see the main Cat_Facts_Tree class.
    """
    print("hit get cat facts")
    res = {}
    if topics == "all":
        res = cftr.fetch()
    else:
        # To make it easy, we'll pass in multiple topics /
        # keys as a single comma-separated string
        _topics = topics.split(",")
        res = cftr.fetch(_topics)
    return jsonify(res)

@app.route("/api/write_new_cat_fact", methods=['POST'])
def write_new_cat_fact():
    """
    Accepts a POST request with payload for "new_cat_facts", 
    which is a list of dictionaries / objects with the required
    keys of depth (integer), parents (comma separated string),
    and fact (string).  
    """
    vals = request.get_json()['new_cat_facts']
    res = cftr.create(vals)
    return jsonify(res)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)