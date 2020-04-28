from __future__ import absolute_import

import argparse
import json
import logging
import os

import apache_beam as beam
import avro
import fastavro
import dataflow_utils

from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from avro import schema

from datetime import datetime
from dataflow_utils import get_schema, clean_csv_int, clean_csv_string, clean_csv_float, clean_csv_boolean, generate_args


class ConvertToDicts(beam.DoFn):
    def process(self, datum):

        ticket_id, name, ticket_num, service, type, create.time, created.by, change.time, closed.time, final.queue, \
        owner, responsible, closed.by, ticket.state, cust.dept, cust.name, priority, comm_article, subject, email, \
        success, removed, closed.HD, closed, last.action, survey.sent, survey.response, enter.andon, enter.default, \
        enter.dr, enter.fq, enter.na, enter.servicedesk, enter.staging, enter.t1, last.andon, last.default, last.dr, \
        last.fq, last.na, last.servicedesk, last.staging, last.t1, start.inventory, start.delivery, enter.ordering, last.ordering, moves, \
        first_com, recent_com, same_com_time, last_move.time, cur_owner.time, open, create.hour, closed.date, create.date, \
        month.open, month.closed, year.open, week.open, week.closed, andon.flag, sd_esc, t1_esc, year, create.weekday, \
        days.open, hrs.own, hrs.update, hrs.triage, hrs.assign, hrs.res, hrs.com, hrs.first_com, team, hyperlink, count, \
        state.new = datum.split(',')

        return [{
            'ticket_id': clean_csv_int(ticket_id),
            'name': clean_csv_string(name),
            'ticket_num': clean_csv_int(ticket_num),
            'service': clean_csv_string(service),
            'type': clean_csv_string(type),
            'create.time': clean_csv_string(create.time),
            'created.by': clean_csv_int(created.by),
            'change.time': clean_csv_string(change.time),
            'closed.time': clean_csv_string(closed.time),
            'final.queue': clean_csv_int(final.queue),
            'owner': clean_csv_string(owner),
            'responsible': clean_csv_string(responsible),
            'closed.by': clean_csv_int(closed.by),
            'ticket.state': clean_csv_string(ticket.state),
            'cust.dept': clean_csv_string(cust.dept),
            'cust.name': clean_csv_string(cust.name),
            'priority': clean_csv_string(priority),
            'comm_article': clean_csv_int(comm_article),
            'subject': clean_csv_string(subject),
            'email': clean_csv_boolean(email),
            'success': clean_csv_boolean(success),
            'removed': clean_csv_int(removed),
            'closed.HD': clean_csv_boolean(closed.HD),
            'closed': clean_csv_boolean(closed),
            'last.action': clean_csv_string(last.action),
            'survey.sent': clean_csv_int(survey.sent),
            'survey.response': clean_csv_int(survey.response),
            'enter.andon': clean_csv_string(enter.andon),
            'enter.default': clean_csv_string(enter.default),
            'enter.dr': clean_csv_string(enter.dr),
            'enter.fq': clean_csv_string(enter.fq),
            'enter.na': clean_csv_string(enter.na),
            'enter.servicedesk': clean_csv_string(enter.servicedesk),
            'enter.staging': clean_csv_string(enter.staging),
            'enter.t1': clean_csv_string(enter.t1),
            'last.andon': clean_csv_string(last.andon),
            'last.default': clean_csv_string(last.default),
            'last.dr': clean_csv_string(last.dr),
            'last.fq': clean_csv_string(last.fq),
            'last.na': clean_csv_string(last.na),
            'last.servicedesk': clean_csv_string(last.servicedesk),
            'last.staging': clean_csv_string(last.staging),
            'last.t1': clean_csv_string(last.t1),
            'start.inventory': clean_csv_string(start.inventory),
            'start.delivery': clean_csv_string(start.delivery),
            'enter.ordering': clean_csv_string(enter.ordering),
            'last.ordering': clean_csv_string(last.ordering),
            'moves': clean_csv_int(moves),
            'first_com': clean_csv_string(first_com),
            'recent_com': clean_csv_string(recent_com),
            'same_com_time': clean_csv_boolean(same_com_time),
            'last_move.time': clean_csv_string(last_move.time),
            'cur_owner.time': clean_csv_string(cur_owner.time),
            'open': clean_csv_boolean(open),
            'create.hour': clean_csv_int(create.hour),
            'closed.date': clean_csv_string(closed.date),
            'create.date': clean_csv_string(create.date),
            'month.open': clean_csv_string(month.open),
            'month.closed': clean_csv_string(month.closed),
            'year.open' : clean_csv_string(year.open),
            'week.open': clean_csv_string(week.open),
            'week.closed': clean_csv_string(week.closed),
            'andon.flag': clean_csv_int(andon.flag),
            'sd_esc': clean_csv_boolean(sd_esc),
            't1_esc': clean_csv_boolean(t1_esc),
            'year': clean_csv_string(year),
            'create.weekday': clean_csv_string(create.weekday),
            'days.open': clean_csv_int(days.open),
            'hrs.own': clean_csv_float(hrs.own),
            'hrs.update': clean_csv_float(hrs.update),
            'hrs.triage': clean_csv_float(hrs.triage),
            'hrs.assign': clean_csv_float(hrs.assign),
            'hrs.res': clean_csv_float(hrs.res),
            'hrs.com': clean_csv_float(hrs.com),
            'hrs.first_com': clean_csv_float(hrs.first_com),
            'team': clean_csv_string(team),
            'hyperlink': clean_csv_string(hyperlink),
            'count': clean_csv_int(count),
            'state.new': clean_csv_string(state.new)
        }]


def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://{}_otrs/tickets/{}/{}/{}_otrs_report_all.csv'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://{}_otrs/tickets/avro_output/{}/{}/{}/avro_output'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened

    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('otrs-tickets-dataflow_scripts',
                                       '{}_otrs'.format(os.environ['GCS_PREFIX']),
                                       'DirectRunner'))

    avro_schema = get_schema('City_of_Pittsburgh_OTRS_Ticket')

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
