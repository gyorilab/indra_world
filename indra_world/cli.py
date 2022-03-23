import json
import argparse


def load_json_file(fname):
    with open(fname, 'r') as fh:
        content = json.load(fh)
    return content


def load_list_file(fname):
    with open(fname, 'r') as fh:
        elements = fh.read().splitlines()
    return elements


def main():
    parser = argparse.ArgumentParser(
        prog='indra_world',
        description="INDRA World assembly CLI")

    group = parser.add_argument_group('Input options')
    gg = group.add_mutually_exclusive_group(required=True)

    gg.add_argument(
        '--reader-output-files', type=str,
        help="Path to a JSON file whose keys are reading system identifiers "
             "and whose values are lists of file paths to outputs from "
             "the given system to be used in assembly.")
    gg.add_argument(
        '--reader-output-dart-query', type=str,
        help="Path to a JSON file that specifies query parameters for "
             "reader output records in DART. Only applicable if DART is "
             "being used.")
    gg.add_argument(
        '--reader-output-dart-keys', type=str,
        help="Path to a text file where each line is a DART storage key "
             "corresponding to a reader output record. Only applicable if "
             "DART is being used.")

    group = parser.add_argument_group('Assembly options')

    group.add_argument(
        '--assembly-config', type=str,
        help="Path to a JSON file that specifies the INDRA assembly pipeline. "
             "If not provided, the default assembly pipeline will be used.")

    gg = group.add_mutually_exclusive_group(required=True)

    gg.add_argument(
        '--ontology-path', type=str, help="Path to an ontology YAML file.")
    gg.add_argument(
        '--ontology-id', type=str,
        help="The identifier of an ontology "
             "registered in DART. Only applicable if DART is being used.")

    group = parser.add_argument_group('Output options')

    group.add_argument(
        '--output-path', type=str, required=True,
        help="The path to a folder to which the INDRA output will be written.")
    group.add_argument(
        '--causemos-output-config', type=str,
        help="Path to a JSON file that provides metadata to be used for a "
             "Causemos-compatible dump of INDRA output (which consists of "
             "multiple files). THe --output-path option must also be used "
             "along with this option.")

    args = parser.parse_args()

    if args.reader_output_files:
        index = load_json_file(args.reader_output_files)
        for reader, files in index.items():
            pass
    elif args.reader_output_dart_query:
        query_args = load_json_file(args.reader_output_dart_query)
        pass
    elif args.reader_output_dart_keys:
        record_keys = load_list_file(args.reader_output_dart_keys)
        pass