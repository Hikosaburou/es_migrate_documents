#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import argparse
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import ElasticsearchException


# Scroll API のエラーを拾うためのカスタムException
# Scroll 処理側で結果を拾ってErrorになった時にraiseする
class ScrollError(ElasticsearchException):
    pass


def ess_client(es_host):
    es = Elasticsearch(
        host=es_host,
        port=443,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
    return es


def scan_search(ess_client, index, scroll, scroll_size):
    """
    Description: Scroll ID を取得する
    """

    # Search_type=scan で scroll_id のみ取得
    # ↑設定すると、ドキュメント以外の情報のみ取得できる
    try:
        response = ess_client.search(
            index=index, search_type="scan", scroll=scroll, size=scroll_size
        )
        scroll_id = response["_scroll_id"]
        total = response["hits"]["total"]
        logger.info("[ {} ] 対象ドキュメント数: {}".format(index, total))
    except Exception as e:
        logger.error(index + u" のscan_searchに失敗しました. [ message = " + str(e) + " ]")

    return scroll_id, total


def scroll_search(ess_client: object, scroll_id: str, scroll: str):
    """
    Description: 対象ドキュメント情報を取得する
    """

    # Scroll API でドキュメントを取得
    response = ess_client.scroll(scroll_id=scroll_id, scroll=scroll)

    if response["_shards"]["failed"]:
        raise ScrollError(
            "Scroll request has failed on {} shards out of {}.".format(
                response["_shards"]["failed"], response["_shards"]["total"]
            )
        )

    docs = response["hits"]["hits"]
    scroll_id = response.get("_scroll_id", None)

    return docs, scroll_id


def gen_bulk_docs(index: str, docs: list):
    """
    Description: Bulk API で追加するためのドキュメントを生成するジェネレータ
    """
    # ジェネレータでデータ列を生成してくれる
    for document in docs:
        # JST表記のタイムスタンプ(タイムゾーン表記なし)をUTCのISO8601形式に直して @timestamp に格納する作業
        yield {
            "_index": index,
            "_type": document["_type"],
            "_id": document["_id"],
            "_source": document["_source"],
        }


def bulk_index(ess_client, index: str, docs: list, dry_run: bool) -> None:
    """
    Description: Bulk Index でインデックスにデータを投入する
    """

    if dry_run:
        return

    helpers.bulk(ess_client, gen_bulk_docs(index, docs))
    time.sleep(0.1)


def main():
    """
    Description: main
    """

    src_es_client = ess_client(args.src_host)
    dst_es_client = ess_client(args.dst_host)

    if args.dry_run:
        logger.info("[ {} ] 接続確認ok".format(src_es_client.info()["cluster_name"]))
        logger.info("[ {} ] 接続確認ok".format(dst_es_client.info()["cluster_name"]))

    scroll_id, total = scan_search(
        src_es_client, args.src_index, args.scroll, args.scroll_size
    )

    while True:
        ### Scroll を繰り返し行い、取得したドキュメントを別のインデックスに投入する
        docs, scroll_id = scroll_search(src_es_client, scroll_id, args.scroll)

        if len(docs) == 0:
            break

        bulk_index(dst_es_client, args.dst_index, docs, args.dry_run)


if __name__ == "__main__":
    # パーサの作成
    psr = argparse.ArgumentParser()

    # コマンドライン引数の設定
    psr.add_argument("--dry-run", default=False, action="store_true")
    psr.add_argument("--src-host", default="localhost")
    psr.add_argument("--src-index", default="test_index")
    psr.add_argument("--dst-host", default="localhost")
    psr.add_argument("--dst-index", default="test_index")
    psr.add_argument("--scroll", default="5m")
    psr.add_argument("--scroll-size", default="20")

    # コマンドラインの引数をパースしてargsに入れる．エラーがあったらexitする
    args = psr.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    main()
