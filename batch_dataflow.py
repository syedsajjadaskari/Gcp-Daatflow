import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteFromText
from apache_beam.options.pipeline_options import Pipeine_Options

class scrip_val(beam.DoFn):
    def process(self,element):
        try:
            line = element.split('"')
            if line[9]=='BUY':
                tp=line[3]+','+line[11].replace(',','')
            else:
                tp=line[3]+',-'+line[11].replace(',','')
            tp=tp.split()            
            return tp
        except:
            logging.INFO('Some Error occured')

def run():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://syed_dataflow_bucket_input',
        help="input file to process"
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help="output file to writw results to"
    )
    know_args,pipeline_args=parser.parse_known_args()
    def sum_groups(word_ones):
        (word,ones)=word_ones
        return word + ',' + str(sum(ones))
    def parse_method(string_input):
        values = re.split(",",re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row=dict(
            zip(('SYMBOL','BUY_SELL_QTY'),values)
        )
        return row
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = p | 'read' >> ReadFromText(known_args.input,skip_header_lines=1)
        counts = (
            lines
            | 'Get required tuple' >> beam.ParDo(scrip_val())
            | 'PwirWithValue' >>    beam.Map(lambda x:(x.split(',')[0],int(x.split(',')[1])))
            | 'Group by key' >>     beam.GroupByKey()
            | 'Sum Group' >>        beam.Map(sum_groups)
            | 'To string' >>        beam.Map(lambda s: str(s))
            | 'String To Bigquery Row' >> beam.Map(lambda s: parse_method(s))

        )
        counts| 'Write TO Bigquery' >> beam.io.Write(
            beam.io.WriteToBigQuery(
                'batch_data',
                dataset='dataflow_demo',
                project='',
                schema ='SYMBOL:STRING,BUY_SELL_QTY:INTEGER',       
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE     )
        )

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()