from flask import Flask, request, jsonify
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, DATETIME
from whoosh.qparser import QueryParser
import os
import pandas as pd
from uuid import uuid4
from datetime import datetime

app = Flask(__name__)
INDEX_DIR = 'indexdir'
CSV_FILE = "posts.csv"


def create_schema():
    schema = Schema(
        id=TEXT(stored=True),
        rubrics=TEXT(stored=True),
        text=TEXT(stored=True),
        created_date=DATETIME(stored=True)
    )
    return schema


def init_index():
    if not os.path.exists(INDEX_DIR):
        os.mkdir(INDEX_DIR)
        schema = create_schema()
        ix = create_in(INDEX_DIR, schema)
        writer = ix.writer()
        df = pd.read_csv(CSV_FILE, encoding='utf-8')
        for _, row in df.iterrows():
            created_date = datetime.strptime(row['created_date'], '%Y-%m-%d %H:%M:%S')
            writer.add_document(
                id=str(uuid4()),
                rubrics=row['rubrics'],
                text=row['text'],
                created_date=created_date
            )
        writer.commit()


def search_documents(query):
    ix = open_dir(INDEX_DIR)
    with ix.searcher() as searcher:
        query_parser = QueryParser("text", ix.schema)
        query_obj = query_parser.parse(query)
        # Сортировка по дате создания
        results = searcher.search(query_obj, limit=20, sortedby='created_date')
        return [(hit['id'], hit['rubrics'], hit['text'], hit['created_date'].strftime('%Y-%m-%d %H:%M:%S')) for hit in results]


def delete_document(doc_id):
    ix = open_dir(INDEX_DIR)
    with ix.searcher() as searcher:
        # Проверка наличия документа
        if not any(hit['id'] == doc_id for hit in searcher.all_stored_fields()):
            print(f"Document with id {doc_id} does not exist.")
            return False

    writer = ix.writer(delete=True)
    writer.delete_by_term('id', doc_id)
    writer.commit()
    print(f"Document with id {doc_id} has been deleted.")
    return True


@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    results = search_documents(query)
    documents = []
    for row in results:
        documents.append({
            "id": row[0],
            "rubrics": row[1],
            "text": row[2],
            "created_date": row[3]
        })

    return jsonify(documents)


@app.route('/delete/<doc_id>', methods=['DELETE'])
def delete(doc_id):
    if delete_document(doc_id):
        return jsonify({"status": "Document deleted"}), 200
    else:
        return jsonify({"error": "Document not found"}), 404


@app.route('/all_documents', methods=['GET'])
def all_documents():
    ix = open_dir(INDEX_DIR)
    with ix.searcher() as searcher:
        results = searcher.all_stored_fields()
        documents = []
        for result in results:
            document = {key: value for key, value in result.items()}
            documents.append(document)
    return jsonify(documents)


@app.route('/')
def home():
    return """
    <h1>Welcome to the Text Search API</h1>
    <p>Use <a href="/search?query=your_query">/search</a> to search for documents.</p>
    <p>Use <a href="/delete/your_doc_id">/delete/&lt;doc_id&gt;</a> to delete a document.</p>
    <p>Use <a href="/all_documents">/all_documents</a> to see all documents in the index.</p>
    """


if __name__ == '__main__':
    if not os.path.exists(INDEX_DIR):
        init_index()
    app.run(debug=True)
