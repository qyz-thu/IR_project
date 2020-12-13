import time
from flask import Flask, request, render_template
from elasticsearch import Elasticsearch
import re
import json
import numpy as np

es = Elasticsearch()
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
p = 2
property_set = {'n', 'np', 'ns', 'ni', 'nz', 'q', 'mq', 't', 'f', 's', 'v', 'a', 'd', 'h', 'k', 'i', 'j', 'r', 'c', 'e', 'g'}


@app.route('/boolean')
def gsearch():
    return render_template('boolean.html')


@app.route('/')
def init():
    return render_template('home.html')


@app.route('/search')
def search():
    start_time = time.time()
    # get input from user
    extended_boolean_search = request.args.get('mode') == "True"
    if extended_boolean_search:
        term1 = request.args.get('term1')
        term2 = request.args.get('term2')
        term3 = request.args.get('term3')
        operator1 = request.args.get('operator1')
        operator2 = request.args.get('operator2')

        text = term1 + ' ' + term2 + ' ' + term3
        query = {'query': {'match': {'raw': text}}}
        res = es.search(index="docs", size=200, body=query, request_timeout=20)

        terms = []
        operators = []
        if term1 is not '':
            terms.append(term1)
        if term2 is not '':
            terms.append(term2)
            operators.append(operator1)
        if term3 is not '':
            terms.append(term3)
            operators.append(operator2)
        operators = [0 if o == "dis" else 1 for o in operators]

        return calculate_similarity(res['hits']['hits'], terms, operators, start_time)
    else:
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


def calculate_similarity(result, terms, operators, start_time):
    assert len(terms) == len(operators) + 1
    query = terms[0]
    for i in range(len(operators)):
        query += '∨' if operators[i] == 1 else '∧'
        query += terms[i + 1]
    idfs = dict()

    final_results = []
    # calculate idf: assign 0.1 for OOV words
    for term in terms:
        if term in idf:
            idfs[term] = np.log10(doc_count / idf[term])
        else:
            idfs[term] = 0.1
    max_idf = max(idfs[x] for x in idfs)
    for res in result:
        segmented_text = res['_source']['segmented']
        text = res['_source']['raw']
        pattern = re.compile('([^\s]+?)_([a-z]+?)')
        words = pattern.findall(segmented_text)
        # doc_len = min(len(words), 5)
        doc_len = 0
        for w in words:
            if w[1] in property_set:
                doc_len += 1
        if doc_len < 5:
            continue
        term_weight = dict()
        for term in terms:
            term_weight[term] = 0.1
        for w in words:
            if w[0] in term_weight:
                term_weight[w[0]] += 1
        for t in term_weight:
            term_weight[t] = term_weight[t] / doc_len * (idfs[t] / max_idf)
        weight = np.array([term_weight[t] for t in term_weight])
        if len(weight) == 2:
            if operators[0] == 0:   # disjunctive
                similarity = 1 - np.power(sum(np.power(1 - weight, p)) / 2, 1 / p)
            elif operators[0] == 1:     # conjunctive
                similarity = np.power(sum(np.power(weight, p)) / 2, 1 / p)
        elif len(weight) == 3:
            if operators[0] == 0 and operators[1] == 0:
                similarity = 1 - np.power(sum(np.power(1 - weight, p)) / 2, 1 / p)
            elif operators[0] == 1 and operators[1] == 1:
                similarity = np.power(sum(np.power(weight, p)) / 2, 1 / p)
            elif operators[0] == 0 and operators[1] == 1:
                temp_sim = 1 - np.power(sum(np.power(1 - weight[:2], p)) / 2, 1 / p)
                x = [temp_sim, weight[2]]
                similarity = np.power(sum(np.power(x, p)) / 2, 1 / p)
            elif operators[0] == 1 and operators[1] == 0:
                temp_sim = np.power(sum(np.power(weight[:2], p)) / 2, 1 / p)
                x = np.array([temp_sim, weight[2]])
                similarity = 1 - np.power(sum(np.power(1 - x, p)) / 2, 1 / p)
        else:
            similarity = 0
        final_results.append([text, similarity])
    final_results.sort(key=lambda x: x[1], reverse=True)
    final_results = [x[0] for x in final_results]
    time_used = time.time() - start_time
    return render_template('result.html', results=final_results[:20], total_num=len(final_results), time=time_used, query=query)


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


# read idf
with open('../idf.json', encoding='utf-8') as f:
    idf = json.load(f)
doc_count = idf['doc_count']
app.run(port=5000, debug=True)




