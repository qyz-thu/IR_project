from datetime import datetime
from flask import Flask, jsonify, request, render_template
from elasticsearch import Elasticsearch
import re

es = Elasticsearch()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/')
def init():
    return render_template('home.html')


# @app.route('/insert_data')
# def insert_data():
#
#     body = {
#         'body': "人民日报爱人民"
#     }
#
#     result = es.index(index='contents', body=body)
#
#     return jsonify(result)


@app.route('/search')
def search():
    keywords1 = request.args.get('keywords1')
    keywords2 = request.args.get('keywords2')
    keywords3 = request.args.get('keywords3')
    property1 = request.args.get('property1')
    property2 = request.args.get('property2')
    property3 = request.args.get('property3')

    text = keywords1 + ' ' + keywords2 + ' ' + keywords3
    query = {'query': {'match': {'raw': text}}}

    res = es.search(index="docs", size=1000, body=query, request_timeout=20)

    keyword = []
    properties = []
    if keywords1 != '':
        keyword.append(keywords1)
        properties.append(property1)
    if keywords2 != '':
        keyword.append(keywords2)
        properties.append(property2)
    if keywords3 != '':
        keyword.append(keywords3)
        properties.append(property3)
    restriction = request.args.get('restriction')

    return filter_result(res['hits']['hits'], keyword, properties, restriction)


def filter_result(result, keyword, property, restriction):
    for res in result:
        segmented_text = res['_source']['segmented']
        pattern = re.compile('(.+?)_([a-z]+?)')
        words = pattern.findall(segmented_text)
        index = 0


    return {1:'hello'}


app.run(port=5000, debug=True)




