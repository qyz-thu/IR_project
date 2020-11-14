from elasticsearch import Elasticsearch, helpers
import time

es = Elasticsearch()


def delete_all():
    query = {'query': {'match_all': {}}}
    es.delete_by_query(index='docs', size=1000, body=query)


def add_document(interval=100000):
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


# delete_all()
# add_document(200000)
