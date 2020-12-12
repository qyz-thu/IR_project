from elasticsearch import Elasticsearch, helpers
import time
import re
import json
es = Elasticsearch()


def delete_all():
    """
    delete all documents
    """
    query = {'query': {'match_all': {}}}
    es.delete_by_query(index='docs', size=1000, body=query)


def add_document(interval=100000):
    """
    add documents to Elasticsearch index using bulk
    """
    index = 0
    stored = 0
    start_time = time.time()
    action = []
    with open('../rmrb', encoding='utf-8') as f1, open('../rmrb_done', encoding='utf-8') as f2:
        for line1, line2 in zip(f1, f2):
            raw = line1.strip()
            segmented = line2.strip()
            index += 1
            stored += 1
            action.append(
                {
                    "_index": "docs",
                    '_id': index,
                    "_source": {
                        "raw": raw,
                        "segmented": segmented
                    }
                }
            )
            if stored >= interval:
                stored = 0
                helpers.bulk(es, action)
                action = []
                print('write %d docs' % index)
                print('time used %.2f s' % (time.time() - start_time))


def get_vocab(file_path, output_path):
    property_set = {'n', 'np', 'ns', 'ni', 'nz', 'q', 'mq', 't', 'f', 's', 'v', 'a', 'd', 'h', 'k', 'i', 'j', 'r', 'c', 'e', 'g'}
    vocab = dict()
    idf = dict()
    pattern = re.compile('([^\s]+?)_([a-z]+?)')
    with open(file_path, encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i % 100000 == 0:
                print('processed %d lines' % i)
            words = pattern.findall(line)
            for word in words:
                if word[1] not in property_set:
                    continue
                vocab[word[0]] = vocab[word[0]] + 1 if word[0] in vocab else 1
            word_set = set([w[0] for w in words])
            for word in word_set:
                idf[word] = idf[word] + 1 if word in idf else 1
        total_count = i

    print(len(vocab))
    filtered_vocab = [x for x in vocab if vocab[x] > 5]
    print(len(filtered_vocab))
    filtered_idf = {'doc_count': total_count}
    for x in filtered_vocab:
        filtered_idf[x] = idf[x]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(filtered_idf, f)


if __name__ == "__main__":
    # delete_all()
    # add_document(500000)
    # get_vocab('../rmrb_done', '../idf.json')
    pass
