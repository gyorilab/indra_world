"""A web application to run INDRA World assembly on a set of reader otputs."""
import os
import logging
import argparse
from flask import jsonify, Flask, render_template, request
from flask_wtf import FlaskForm
from wtforms import SubmitField, validators, SelectMultipleField, \
    StringField, TextAreaField, SelectField
from wtforms.fields.html5 import DateField
from flask_bootstrap import Bootstrap

from indra.config import get_config
from indra_world.sources.dart import DartClient, prioritize_records
from indra_world.service.corpus_manager import CorpusManager


DB_URL = get_config('INDRA_WM_SERVICE_DB', failure_ok=False)
dart_client = DartClient()


logger = logging.getLogger('indra_wm_service.assembly_dashboard')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_key'
Bootstrap(app)


reader_names = [('eidos', 'eidos'),
                ('hume', 'hume'),
                ('sofia', 'sofia')]

assembly_levels = [('grounding', 'grounding'),
                   ('location', 'location'),
                   ('location_and_time', 'location_and_time')]


class RunAssemblyForm(FlaskForm):
    """Defines the main input form constituting the dashboard."""
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
    assembly_config_name = StringField(label='Assembly configuration name')
    corpus_descr = TextAreaField(label='Corpus description',
                                 validators=[validators.input_required()])
    assembly_level = SelectField(label='Level of assembly',
                                 choices=assembly_levels)
    submit_button = SubmitField('Run assembly')


@app.route('/', methods=['GET'])
def index():
    """Render landing page form."""
    run_assembly_form = RunAssemblyForm()
    kwargs = {'run_assembly_form': run_assembly_form}
    return render_template('index.html', **kwargs)


@app.route('/run_assembly', methods=['POST'])
def run_assembly():
    """Run assembly."""
    readers = request.form.getlist('readers')
    readers = readers if readers else None
    after_date = request.form.get('after_date')
    before_date = request.form.get('before_date')
    corpus_id = request.form.get('corpus_id')
    corpus_name = request.form.get('corpus_name')
    corpus_descr = request.form.get('corpus_descr')
    assembly_level = request.form.get('assembly_level')
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

    records = prioritize_records(records, reader_priorities)

    num_docs = len({rec['document_id'] for rec in records})

    meta_data = {
        'corpus_id': corpus_id,
        'description': corpus_descr,
        'display_name': corpus_name,
        'readers': readers,
        'assembly': {
            'level': assembly_level,
            'grounding_threshold': 0.7,
        },
        'num_documents': num_docs
    }

    cm = CorpusManager(db_url=DB_URL,
                       corpus_id=corpus_id,
                       metadata=meta_data)
    cm.dump_s3()
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
