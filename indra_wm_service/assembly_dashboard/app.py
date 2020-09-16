import os
import logging
import argparse
from flask import jsonify, abort, Response, Flask, render_template, request, \
    redirect, session
from flask_wtf import FlaskForm
from wtforms import SubmitField, validators, SelectMultipleField, \
    StringField, TextAreaField, SelectField
from wtforms.fields.html5 import DateField
from flask_bootstrap import Bootstrap

from indra.pipeline import AssemblyPipeline
from indra_wm_service.assembly.operations import *
from indra_wm_service.assembly.dart import process_reader_outputs
from indra_wm_service.live_curation import Corpus

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ASSEMBLY_JSON = os.path.join(HERE, 'default_pipeline.json')
DART_STORAGE = os.environ.get('DART_STORAGE')


logger = logging.getLogger('indra_wm_service.assembly_dashboard')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_key'
Bootstrap(app)


reader_names = [('eidos', 'eidos'),
                ('hume', 'hume'),
                ('cwms', 'cwms'),
                ('sofia', 'sofia')]

assembly_levels = [('grounding', 'grounding'),
                   ('location', 'location'),
                   ('location_and_time', 'location_and_time')]


class RunAssemblyForm(FlaskForm):
    after_date = DateField(label='After date', format='%Y-%m-%d')
    before_date = DateField(label='Before date', format='%Y-%m-%d')
    readers = SelectMultipleField(label='Readers',
                                  id='reader-select',
                                  choices=reader_names,
                                  validators=[validators.input_required()])
    corpus_id = StringField(label='Corpus ID',
                            validators=[validators.input_required()])
    corpus_name = StringField(label='Corpus display name',
                              validators=[validators.input_required()])
    corpus_descr = TextAreaField(label='Corpus description',
                                 validators=[validators.input_required()])
    assembly_level = SelectField(label='Level of assembly',
                                 choices=assembly_levels)
    submit_button = SubmitField('Run assembly')


@app.route('/', methods=['GET'])
def index():
    run_assembly_form = RunAssemblyForm()
    kwargs = {'run_assembly_form': run_assembly_form}
    return render_template('index.html', **kwargs)


@app.route('/run_assembly', methods=['POST'])
def run_assembly():
    readers = request.form.getlist('readers')
    readers = readers if readers else None
    after_date = request.form.get('after_date')
    before_date = request.form.get('before_date')
    corpus_id = request.form.get('corpus_id')
    corpus_name = request.form.get('corpus_name')
    corpus_descr = request.form.get('corpus_descr')
    assembly_level = request.form.get('assembly_level')
    from indra.literature import dart_client
    timestamp = None if (not before_date and not after_date) else {}
    if after_date:
        timestamp['after'] = after_date
    if before_date:
        timestamp['before'] = before_date
    logger.info('Fetching reader output for readers %s and dates %s' %
                (str(readers), str(timestamp)))

    # TODO: make this parameterizable
    reader_priorities = {}

    records = dart_client.get_reader_output_records(readers,
                                                    timestamp=timestamp)
    if not records:
        return jsonify({})

    records = dart_client.prioritize_records(records, reader_priorities)

    num_docs = len({rec['document_id'] for rec in records})

    reader_outputs = dart_client.download_records(records,
                                                  local_storage=DART_STORAGE)

    if not reader_outputs:
        return jsonify({})

    logger.info('Processing reader output...')
    stmts = process_reader_outputs(reader_outputs)
    logger.info('Got a total of %s statements' % len(stmts))

    if not stmts:
        return jsonify({})

    pipeline = AssemblyPipeline.from_json_file(DEFAULT_ASSEMBLY_JSON)
    assembled_stmts = pipeline.run(stmts)

    meta_data = {
        'corpus_id': corpus_id,
        'description': corpus_descr,
        'display_name': corpus_name,
        'readers': readers,
        'assembly': {
            'level': assembly_level,
            'grounding_threshold': 0.7,
        },
        'num_statements': len(assembled_stmts),
        'num_documents': num_docs
    }

    corpus = Corpus(corpus_id=corpus_id,
                    statements=assembled_stmts,
                    raw_statements=stmts,
                    meta_data=meta_data)
    corpus.s3_put()
    return 'Assembly complete'


if __name__ == '__main__':
    # Process arguments
    parser = argparse.ArgumentParser(
        description='Choose a corpus for live curation.')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', default=8001, type=int)
    args = parser.parse_args()

    # Run the app
    app.run(host=args.host, port=args.port, threaded=False)
