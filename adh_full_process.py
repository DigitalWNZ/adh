from flask_restful import request,Resource
from flask import Response
import os
import json
import time
import pandas as pd
from googleapiclient.discovery import build
from google.cloud import logging
from google.cloud.logging import Resource
from typing import Any, Dict, Generator, List, Optional, Text, Mapping
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials

class adhHelper(object):
    """ADH API Helper"""

    def __init__(self, input_adh_service):
        self.adh_service = input_adh_service

    def listCustomersAccountInformation(self) -> List[Mapping[Text, Any]]:
        """List all account information for current service account"""
        return self.adh_service.customers().list().execute()

    def getAllAnalsysisQueries(self, cid: Text) -> List[Mapping[Text, Any]]:
        """Get all analysis queries.

        Delete analysis queries, the detail
        link as below:
        https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/list

        Args:
          cid: customer's id, ex customers/1234567.

        Returns:
          list contain all the analysis queries object
        """
        all_analysis_queries = []
        list_result = self.adh_service.customers().analysisQueries().list(parent=cid).execute()
        all_analysis_queries.extend(list_result['queries'])
        while list_result.get('nextPageToken'):
            list_result = self.adh_service.customers().analysisQueries().list(parent=cid,
                                                                              pageToken=list_result[
                                                                                  'nextPageToken']).execute()
            all_analysis_queries.extend(list_result['queries'])
        return all_analysis_queries

    def getAnalsysisQueryByID(self, query_uri: Text) -> List[Mapping[Text, Any]]:
        """Get analysis query by name.

        Args:
          query_uri: customers/****/analysisQueries/****

        Returns:
          json contains the query information
        """

        json_result = self.adh_service.customers().analysisQueries().get(name=query_uri).execute()
        return json_result

    def deleteAnalysisQueries(self, name: Text) -> None:
        """Delete analysis queries.

        Delete analysis queries, the detail
        link as below:
        https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/delete

        Args:
          name: analysis query name
        """
        return self.adh_service.customers().analysisQueries().delete(name=name).execute()

    def createAnalysisQueries(self, cid: Text, title: Text, query_txt: Text):
        """Create analysis queries.

        Create analysis queries, the detail
        link as below:
        https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/create

        Args:
          cid: customer's id, ex customers/1234567.
          title: analysis's queries.
          query_txt: adh sql query

        Returns:
          analysis query creation object, will contain analysis query name

        """
        # Compose analysis queries creation body
        analysisQueriesBody = {
            "title": title,
            "queryText": query_txt
        }

        # Create analysis queries via adh service
        aq_object = self.adh_service.customers().analysisQueries().create(parent=cid,
                                                                          body=analysisQueriesBody).execute()
        return aq_object

    def startAnalysisQueries(self, cid: Text, name: Text, start_date: Text,
                             end_date: Text, output_table_name: Text):
        """Start analysis queries.

        Start analysis queries, the detail
        link as below:
        https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/start

        Args:
          cid: customer's id, ex customers/1234567.
          name: analysis query name
          start_date: The start date (inclusive) for the query, ex 2020-01-01.
          end_date: The end date (inclusive) for the query, ex 2020-01-31.
          output_table_name: Destination BigQuery table for query results with the format 'project.dataset.table_name'.

        Returns:
          operation object

        """
        # convert start_date and end_date to int format
        start_year = int(start_date.split('-')[0])
        start_month = int(start_date.split('-')[1])
        start_day = int(start_date.split('-')[2])

        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])
        end_day = int(end_date.split('-')[2])

        # Compose analysis queries start body
        analysisQueriesExecuteBody = {
            "spec":
                {
                    "startDate": {
                        "year": start_year,
                        "month": start_month,
                        "day": start_day
                    },
                    "endDate": {
                        "year": end_year,
                        "month": end_month,
                        "day": end_day
                    }
                },
            "destTable": output_table_name
        }
        operation_obj = self.adh_service.customers().analysisQueries().start(name=name,
                                                                             body=analysisQueriesExecuteBody).execute()
        return operation_obj

    def queryOperationStatus(self, name: Text) -> Mapping[Text, Any]:
        """Query operation status.

        query operation status, the detail
        link as below:
        https://developers.google.com/ads-data-hub/reference/rest/v1/operations/get

        Args:
          name: operation name

        Returns:
          operation object

        """
        return self.adh_service.operations().get(name=name).execute()

    def getDataFromBigQuery(self, dst_table_name: Text) -> pd.DataFrame:
        """Get table data from BigQuery.

        Args:
          dst_table_name: table format 'project.dataset.table_name'

        Returns:
          data frame

        """
        # Separate project id from dst_table_name
        project_id = dst_table_name.split('.')[0]
        result = []

        # Init bigquery client
        bq_client = bigquery.Client(project=project_id)
        exec_sql = "select * from `%s`" % dst_table_name

        print("Get data from bigquery, SQL: [%s]" % exec_sql)

        # init bigquery job
        job_config = bigquery.QueryJobConfig()
        query_job = bq_client.query(exec_sql, location='US', job_config=job_config)

        for row in query_job:
            result.append(dict(row.items()))

        assert query_job.state == 'DONE'
        pd_result = pd.DataFrame(result)
        return pd_result

    def createAndStartAnalysisQueries(self, cid: Text, title: Text,
                                      query_txt: Text, start_date: Text,
                                      end_date: Text, output_table_name: Text) -> pd.DataFrame:
        """Create and start analysis queries.

        Args:
          cid: customer's id, ex customers/1234567.
          title: analysis's queries.
          query_txt: adh sql query
          start_date: The start date (inclusive) for the query, ex 2020-01-01.
          end_date: The end date (inclusive) for the query, ex 2020-01-31.
          output_table_name: Destination BigQuery table for query results with the format 'project.dataset.table_name'.

        Returns:
          data frame

        """
        # Retrieve all existing analysis queries from ADH
        all_analysis_queries = self.getAllAnalsysisQueries(cid)

        # If any existing analysis queries have the same title with the given
        # parameter, analysis queries will be deleted.
        existing_analysis_queries = [query for query in all_analysis_queries if query['title'] == title]
        for existing_aq in existing_analysis_queries:
            self.deleteAnalysisQueries(existing_aq['name'])

        # Create analysis queries
        aq_object = self.createAnalysisQueries(cid, title, query_txt)

        # Start analysis queries
        operation_obj = self.startAnalysisQueries(cid, aq_object['name'],
                                                  start_date, end_date,
                                                  output_table_name)

        # Wait for operation done
        while operation_obj.get('done', False) is False:
            operation_obj = self.queryOperationStatus(operation_obj['name'])
            time.sleep(5)

        return self.getDataFromBigQuery(output_table_name)
# def ret_msg(status,msg,status_code):
#     return Response(json.dumps({'status': status, 'message': msg}), status=status_code)

def output_to_cloud_console(msg):
    log_client = logging.Client()
    log_name = 'cloudfunctions.googleapis.com%2Fcloud-functions'

    # Inside the resource, nest the required labels specific to the resource type
    res = Resource(type="cloud_function",
                   labels={
                       "function_name": "adh_function",
                       "region": "us-central1"
                   },
                   )
    logger = log_client.logger(log_name.format("allen-first"))
    logger.log_struct(
        {"message": msg}, resource=res, severity='ERROR')

def main_function(request):
    request_json = request.get_json()
    try:
        gcp_api_key = request_json['gcp_api_key']
        gcp_api_scope =request_json['gcp_api_scope']
        adh_api_name = request_json['adh_api_name']
        adh_api_version = request_json['adh_api_version']
        gcp_sa_cred_file=request_json['gcp_sa_cred_file']
        customerID = request_json['customerID']
        action=request_json['action']
        try:
            output_to_cloud_console = request_json['output_to_cloud_console']
        except Exception as e:
            output_to_cloud_console = 0
    except Exception as e:
        msg='Errors happend when parsing mandatory parameter'
        output_to_cloud_console(msg)
        return msg

    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_sa_cred_file
        gcp_sa_credentials = ServiceAccountCredentials.from_json_keyfile_name(gcp_sa_cred_file, scopes=gcp_api_scope)
        adh_service = build(adh_api_name, adh_api_version,
                            credentials=gcp_sa_credentials,
                            developerKey=gcp_api_key)
        myADHHelper = adhHelper(adh_service)
        customerInfo = json.loads(json.dumps(myADHHelper.listCustomersAccountInformation()))
        print(pd.DataFrame(customerInfo['customers'], columns=["customerId", "displayName"]))
        adh_customer_id='customers/' + customerID

    except Exception as e:
        msg='Errors happend when building the ADH service.'
        if output_to_cloud_console == 1:
            output_to_cloud_console(msg)
        return msg

    if action=='create':
        try:
            query_filename=request_json['query_filename']
        except Exception as e:
            msg='No query file name specified for create action'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg

        try:
            query_name = request_json['query_name']
        except Exception as e:
            # return ret_msg('Failed', 400, 'No query name specified for create action')
            msg='No query name specified for create action'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg
        with open(query_filename, 'r') as f:
            query_text = f.read()
            aq_result = myADHHelper.createAnalysisQueries(adh_customer_id, query_name, query_text)
            msg = json.dumps(aq_result)
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg
            # print(aq_result)
    elif action == 'start':
        try:
            query_id=request_json['query_id']
        except Exception as e:
            # return ret_msg('Failed', 400, 'No query id specified for start action')
            msg = 'No query id specified for start action'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg

        try:
            start_date=request_json['start_date']
            end_date=request_json['end_date']
            output_table=request_json['output_table']
        except Exception as e:
            # return ret_msg('Failed', 400, 'Please specify start/end date and output_table.')
            msg = 'Please specify start/end date and output_table.'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg

        query_uri='customers/' + customerID + '/analysisQueries/' + query_id
        result = myADHHelper.startAnalysisQueries(adh_customer_id, query_uri, start_date, end_date,output_table)
        msg = json.dumps(result)
        if output_to_cloud_console == 1:
            output_to_cloud_console(msg)
        return msg
        # print(result)
    elif action=='delete':
        pass
    elif action == 'get_query':
        try:
            query_id=request_json['query_id']
        except Exception as e:
            # return ret_msg('Failed', 400, 'No query id specified for start action')
            msg = 'No query id specified for start action'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg
        query_uri = 'customers/' + customerID + '/analysisQueries/' + query_id
        result= myADHHelper.getAnalsysisQueryByID(query_uri)
        # print(result)
        msg = json.dumps(result)
        print(msg)
        if output_to_cloud_console == 1:
            output_to_cloud_console(msg)
        return msg
    elif action=='get_op':
        try:
            op_id=request_json['op_id']
        except Exception as e:
            # return ret_msg('Failed', 400, 'No query id specified for start action')
            msg = 'No query id specified for start action'
            if output_to_cloud_console == 1:
                output_to_cloud_console(msg)
            return msg
        op_uri='operations/' + op_id
        op_status = myADHHelper.queryOperationStatus(op_uri)
        msg = json.dumps(op_status)
        if output_to_cloud_console == 1:
            output_to_cloud_console(msg)
        return msg
        # print(op_status)
    else:
        # return ret_msg('Failed', 400, 'Action must be in create/start/delete/get_query/get_op')
        msg = 'Action must be in create/start/delete/get_query/get_op'
        if output_to_cloud_console == 1:
            output_to_cloud_console(msg)
        return msg









