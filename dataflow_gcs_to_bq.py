import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime, timezone
import csv


VALID_TAG = 'valid'
INVALID_TAG = 'invalid'


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', type=str, help='Path to input file')
        parser.add_argument('--dataset', type=str, help='BigQuery dataset')


def parse_csv(line):
    for row in csv.reader([line]):
        return {
            'name': row[0].strip(),
            'age': row[1].strip(),
            'email': row[2].strip()
        }


def validate_row(row):
    try:
        age = int(row['age'])
        if row['name'] and '@' in row['email']:
            row['age'] = age
            yield beam.pvalue.TaggedOutput(VALID_TAG, row)
        else:
            yield beam.pvalue.TaggedOutput(INVALID_TAG, row)
    except:
        yield beam.pvalue.TaggedOutput(INVALID_TAG, row)


def create_audit_log(count, file_name):
    yield {
        'file_name': file_name,
        'time_of_ingestion': datetime.now(timezone.utc).isoformat(),
        'record_count': count
    }


def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    project = pipeline_options.get_all_options().get('project')

    custom_options = pipeline_options.view_as(CustomOptions)
    input_file = custom_options.input
    dataset = custom_options.dataset

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)

        parsed = lines | 'Parse CSV' >> beam.Map(parse_csv)

        validated = parsed | 'Validate Rows' >> beam.FlatMap(validate_row).with_outputs(VALID_TAG, INVALID_TAG)

        validated[VALID_TAG] | 'Write Valid Rows' >> beam.io.WriteToBigQuery(
            table=f'{project}:{dataset}.main_table',
            schema='name:STRING, age:INTEGER, email:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        validated[INVALID_TAG] | 'Write Invalid Rows' >> beam.io.WriteToBigQuery(
            table=f'{project}:{dataset}.error_table',
            schema='name:STRING, age:STRING, email:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        counted = validated[VALID_TAG] | 'Count Valid Rows' >> beam.combiners.Count.Globally()

        audit = counted | 'Create Audit Log' >> beam.FlatMap(create_audit_log, file_name=input_file)

        audit | 'Write Audit Log' >> beam.io.WriteToBigQuery(
            table=f'{project}:{dataset}.audit_log_table',
            schema='file_name:STRING, time_of_ingestion:TIMESTAMP, record_count:INTEGER',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


if __name__ == '__main__':
    run()
