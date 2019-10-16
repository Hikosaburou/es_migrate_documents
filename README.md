# es_migrate_documents
間違えて別のクラスタに流してしまったドキュメントを移行する


## 前提

Amazon Elasticsearch Service のパブリックアクセスドメインを前提にしているため、HTTPSアクセスが前提。以下の環境への接続は不可能。

- AWSクレデンシャル設定の必要な環境
- Elasticsearchの標準ポートへのアクセス


## Pipenv Settings

PipenvでPythonモジュールのバージョン固定を行なっている。インストール方法は[ここ](https://dev.classmethod.jp/etc/environment_to_pipenv-pyenv/)などを参照。

以下コマンドでPython実行環境を準備する (要pyenv)

```
pipenv sync
```

## Usage

```
pipenv run ./es_migrate_documents.py \
  --src-host <source host name>
  --src-index <source index name>
  --dst-host <destination host name>
  --dst-index <destination index name>
```

### ex

```
pipenv run ./es_migrate_documents.py \
  --src-host example-src-xxxxxxxxxxxxxxxxxxxxxxxx.ap-northeast-1.es.amazoneaws.com
  --src-index example-src-test
  --dst-host example-dst-yyyyyyyyyyyyyyyyyyyyyyyy.ap-northeast-1.es.amazoneaws.com
  --dst-index example-dst-test
```