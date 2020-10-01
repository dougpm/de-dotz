import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options import pipeline_options
            
class FileHandler(object):
   
    def __init__(self, filepath, fields):

        self.filepath = filepath
        self.fields = fields.split(",")

    def parse(self, row):

        values = re.split(",", re.sub(r'[\r\n"]', '', row))
        row = dict(list(zip(self.fields, values)))
        return row

    def create_schema(self):

        schema = ",".join(field + ":STRING" for field in self.fields)
        return schema

def run(argv=None):
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--file_path', dest='file_path', required=False,
                        help='GCS path for the csv file')
    parser.add_argument('--fields', dest='fields', required=False,
                        help='Header fields on the csv')
    parser.add_argument('--destination_table_id', dest='destination_table_id', required=False,
                        help='ID of the table to be created in BigQuery')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    p_opts = pipeline_options.PipelineOptions(pipeline_args)

    file_handler = FileHandler(known_args.file_path, known_args.fields)
    schema = file_handler.create_schema()

    with beam.Pipeline(options=p_opts) as p:
        (p
            | 'Read csv file' >> beam.io.ReadFromText(known_args.file_path, skip_header_lines=1)       
            | 'Format rows to BQ' >> beam.Map(lambda r: file_handler.parse(r))
            | 'Write to BQ' >> beam.io.WriteToBigQuery(known_args.destination_table_id,
                                                schema=schema,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )
        
                
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()