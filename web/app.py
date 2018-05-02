from flask import Flask, render_template, request, jsonify
from utils import get_redis_conn, get_MySQL_conn, mymd5, find_keyword
import json
import redis
import sys
import jieba

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/search', methods=['GET'])
def searchResult():
    return render_template('search_result.html')


@app.route('/searchApi/<keyword>', methods=['GET'])
def search(keyword):
    keyword_list = jieba.lcut(keyword, cut_all=False)
    result_dict = dict()
    cur = mysql_conn.cursor()
    for word in keyword_list:
        if word.encode() in all_word_set:
            url_list = [
                i.decode()
                for i in redis_conn.lrange(word + '_url_list', 0, -1)
            ]
            tfidf_list = [
                float(i.decode())
                for i in redis_conn.lrange(word + '_tfidf_list', 0, -1)
            ]
            for url in url_list:
                if url in result_dict:
                    result_dict[url] += (1 + tfidf_list[url_list.index(url)])
                else:
                    result_dict[url] = tfidf_list[url_list.index(url)]
    sorted_url_list =[[url_tuple[0], redis_conn.hget('url2title', url_tuple[0]).decode()]\
                    for url_tuple in sorted(result_dict.items(), key=lambda d:d[1], reverse=True)[:20]]

    for url in sorted_url_list:
        cur.execute("""select content from url_hash where url_hash=%s""", (mymd5(url[0]),))
        url.append(find_keyword(cur.fetchone()[0], keyword_list))

    return jsonify(
        dict(
            lengths=len(sorted_url_list),
            urls=sorted_url_list,
            keyword=keyword,
            keyword_list=keyword_list)), 200


if __name__ == '__main__':
    redis_conn = get_redis_conn(sys.argv[1])
    mysql_conn = get_MySQL_conn(sys.argv[1])
    all_word_set = redis_conn.smembers('all_word_list')
    app.run(debug=True)