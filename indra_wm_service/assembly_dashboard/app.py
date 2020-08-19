import logging
import argparse
from flask import jsonify, abort, Response, Flask, render_template, request, \
    redirect, session
from flask_wtf import FlaskForm
from wtforms import SubmitField, validators, SelectMultipleField, DateField, \
    StringField
from flask_bootstrap import Bootstrap


logger = logging.getLogger('indra_wm_service.assembly_dashboard')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_key'
Bootstrap(app)


reader_names = [('eidos', 'eidos'),
                ('hume', 'hume'),
                ('cwms', 'cwms'),
                ('sofia', 'sofia')]


class RunAssemblyForm(FlaskForm):
    after_date = DateField(label='After date')
    before_date = DateField(label='Before date')
    readers = SelectMultipleField(label='Readers',
                                  id='reader-select',
                                  choices=reader_names,
                                  validators=[validators.unicode_literals])
    corpus_id = StringField(label='Corpus key')
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
    from indra.literature import dart_client
    timestamp = None if (not before_date and not after_date) else {}
    if after_date:
        timestamp['after'] = after_date
    if before_date:
        timestamp['before'] = before_date
    logger.info('Fetching reader output for readers %s and dates %s' %
                (str(readers), str(timestamp)))
    reader_outputs = dart_client.get_reader_outputs(readers, timestamp=timestamp)





if __name__ == '__main__':
    # Process arguments
    parser = argparse.ArgumentParser(
        description='Choose a corpus for live curation.')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', default=8001, type=int)
    args = parser.parse_args()

    # Run the app
    app.run(host=args.host, port=args.port, threaded=False)
