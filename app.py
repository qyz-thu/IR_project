import time
from flask import Flask, jsonify, request, render_template
from elasticsearch import Elasticsearch
import re

es = Elasticsearch()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/')
def init():
    return render_template('home.html')


@app.route('/search')
def search():
    start_time = time.time()
    # get input from user
    keywords1 = request.args.get('keywords1')
    keywords2 = request.args.get('keywords2')
    keywords3 = request.args.get('keywords3')
    property1 = request.args.get('property1')
    property2 = request.args.get('property2')
    property3 = request.args.get('property3')

    text = keywords1 + ' ' + keywords2 + ' ' + keywords3
    query = {'query': {'match': {'raw': text}}}

    res = es.search(index="docs", size=100, body=query, request_timeout=20)

    keyword = []
    properties = []
    if keywords1 != '':
        keyword.append(keywords1)
        properties.append('' if property1 == 'o' else property1)    # deal with 'others' property
    if keywords2 != '':
        keyword.append(keywords2)
        properties.append('' if property2 == 'o' else property2)
    if keywords3 != '':
        keyword.append(keywords3)
        properties.append('' if property3 == 'o' else property3)
    restriction = request.args.get('restriction')

    return filter_result(res['hits']['hits'], keyword, properties, restriction, start_time)


def filter_result(result, keyword, property, restriction, start_time):
    """
    filter the results from Elasticsearch according to user input
    """
    final_result = []
    assert len(keyword) == len(property)
    for res in result:
        segmented_text = res['_source']['segmented']
        segmented_text = re.sub('_n[psiz]', '_n', segmented_text)
        _segmented_text = ' ' + segmented_text
        text = res['_source']['raw']
        pattern = re.compile('([^\s]+?)_([a-z]+?)')
        words = pattern.findall(segmented_text)
        if restriction == '0':    # no restriction
            targets = [' ' + keyword[i] + '_' + property[i] for i in range(len(keyword))]
            ok = True
            for target in targets:
                if _segmented_text.find(target) == -1:
                    ok = False
                    break
            if ok:
                final_result.append(text)
        elif restriction == '1':  # neighboring
            match_position = [[] for _ in range(len(keyword))]
            for i, w in enumerate(words):
                for j, k in enumerate(keyword):
                    if k == w[0] and (property[j] == w[1] or property[j] == ''):
                        match_position[j].append(i)
            if len(keyword) == 1:
                if len(match_position[0]) > 0:
                    final_result.append(text)
            elif len(keyword) == 2:
                for x in match_position[0]:
                    if (x+1) in match_position[1] or (x-1) in match_position[1]:
                        final_result.append(text)
                        break
            elif len(keyword) == 3:
                for x in match_position[0]:
                    if ((x-1) in match_position[1] and (x-2) in match_position[2]) or ((x-2) in match_position[1] and (x-1) in match_position[2])\
                    or ((x-1) in match_position[1] and (x+1) in match_position[2]) or ((x+1) in match_position[1] and (x-1) in match_position[2])\
                    or ((x+1) in match_position[1] and (x+2) in match_position[2]) or ((x+1) in match_position[1] and (x+2) in match_position[2]):
                        final_result.append(text)
                        break
        elif restriction == '2':  # ordered
            index = 0
            for word in words:
                if keyword[index] == word[0] and (property[index] == '' or property[index] == word[1]):
                    index += 1
                if index == len(keyword):
                    final_result.append(text)
                    break
        elif restriction == '3':  # ordered and neighboring
            match_position = [[] for _ in range(len(keyword))]
            for i, w in enumerate(words):
                for j, k in enumerate(keyword):
                    if k == w[0] and (property[j] == w[1] or property[j] == ''):
                        match_position[j].append(i)
            if len(keyword) == 1:
                if len(match_position[0]) > 0:
                    final_result.append(text)
            elif len(keyword) == 2:
                for x in match_position[0]:
                    if (x + 1) in match_position[1]:
                        final_result.append(text)
                        break
            elif len(keyword) == 3:
                for x in match_position[0]:
                    if (x + 1) in match_position[1] and (x + 2) in match_position[2]:
                        final_result.append(text)
                        break

    time_used = time.time() - start_time
    return render_template('result.html', results=final_result[:20], total_num=len(final_result), time=time_used)


app.run(port=5000, debug=True)




