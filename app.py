from flask import Flask, request, jsonify
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, DATETIME
from whoosh.qparser import QueryParser
import os
import pandas as pd
from uuid import uuid4
from datetime import datetime
from flask_restx import Api, Resource, fields

app = Flask(__name__)
api = Api(app, version='1.0', title='Text Search API',
          description='API для поиска и управления документами с использованием Whoosh')

ns = api.namespace('documents', description='Операции с документами')

INDEX_DIR = 'indexdir'
CSV_FILE = r"C:\Users\sokol\Downloads\posts.csv"


document_model = api.model('Document', {
    'id': fields.String(required=True, description='ID документа'),
    'rubrics': fields.String(required=True, description='Рубрики'),
    'text': fields.String(required=True, description='Текст документа'),
    'created_date': fields.String(required=True, description='Дата создания документа')
})


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
        results = searcher.search(query_parser.parse(query), limit=20, sortedby='created_date')
        return [(hit['id'], hit['rubrics'], hit['text'], hit['created_date'].strftime('%Y-%m-%d %H:%M:%S')) for hit in results]


def delete_document(doc_id):
    ix = open_dir(INDEX_DIR)
    with ix.searcher() as searcher:
        if not any(hit['id'] == doc_id for hit in searcher.all_stored_fields()):
            return False
    writer = ix.writer(delete=True)
    writer.delete_by_term('id', doc_id)
    writer.commit()
    return True


@ns.route('/search')
class SearchDocument(Resource):
    @api.doc(params={'query': 'Строка поиска'})
    @api.marshal_list_with(document_model)
    def get(self):
        """Поиск документов по строке запроса"""
        query = request.args.get('query')
        if not query:
            api.abort(400, "Query parameter is required")
        results = search_documents(query)
        return [{'id': row[0], 'rubrics': row[1], 'text': row[2], 'created_date': row[3]} for row in results]


@ns.route('/delete/<string:doc_id>')
class DeleteDocument(Resource):
    @api.doc(params={'doc_id': 'ID документа для удаления'})
    def delete(self, doc_id):
        """Удаление документа по ID"""
        if delete_document(doc_id):
            return {'status': 'Document deleted'}, 200
        else:
            api.abort(404, "Document not found")


@ns.route('/all')
class GetAllDocuments(Resource):
    @api.marshal_list_with(document_model)
    def get(self):
        """Получить все документы"""
        ix = open_dir(INDEX_DIR)
        with ix.searcher() as searcher:
            results = searcher.all_stored_fields()
            return [dict(result) for result in results]


if __name__ == '__main__':
    if not os.path.exists(INDEX_DIR):
        init_index()
    app.run(debug=True)
