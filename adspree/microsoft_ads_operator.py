import shutil
from pathlib import Path
from typing import Any, List
import logging
import stringcase

import pandas as pd
from airflow.models import Connection
from bingads import ServiceClient, AuthorizationData, OAuthDesktopMobileAuthCodeGrant
from bingads.v13.reporting import ReportingServiceManager, ReportingDownloadParameters

from gcs import GCSBaseOperator


class MicrosoftAdsToGCSOperator(GCSBaseOperator):

    template_fields = ["bucket", "object_name", "date_from", "date_to"]
    template_fields_renderers = {"bucket": "py", "object_name": "py", "date_from": "py", "date_to": "py"}

    def __init__(self, *, msads_connection_id: str, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.msads_connection_id = msads_connection_id
        self.date_from = date_from
        self.date_to = date_to
        self.columns = [
            "AccountName",
            "AccountNumber",
            "AccountId",
            "TimePeriod",
            "CampaignName",
            "CampaignId",
            "AdGroupName",
            "AdGroupId",
            "AdId",
            "CurrencyCode",
            "AdDistribution",
            "DestinationUrl",
            "Impressions",
            "Clicks",
            "Ctr",
            "AverageCpc",
            "Spend",
            "AveragePosition",
            "Conversions",
            "ConversionRate",
            "CostPerConversion",
            "DeviceType",
            "Language",
            "BidMatchType",
            "DeliveredMatchType",
            "Network",
            "TopVsOther",
            "DeviceOS",
            "Assists",
            "Revenue",
            "ReturnOnAdSpend",
            "CostPerAssist",
            "RevenuePerConversion",
            "RevenuePerAssist",
            "TrackingTemplate",
            "CustomParameters",
            "FinalUrl",
            "FinalMobileUrl",
            "FinalAppUrl",
            "AccountStatus",
            "CampaignStatus",
            "AdGroupStatus",
            "AdStatus",
            "CustomerId",
            "CustomerName",
            "FinalUrlSuffix",
            "AllConversions",
            "AllRevenue",
            "AllConversionRate",
            "AllCostPerConversion",
            "AllReturnOnAdSpend",
            "AllRevenuePerConversion",
            "ViewThroughConversions",
            "Goal",
            "GoalType",
            "AbsoluteTopImpressionRatePercent",
            "TopImpressionRatePercent",
            "AverageCpm",
        ]

    def authorize(self) -> AuthorizationData:

        connection = Connection.get_connection_from_secrets(self.msads_connection_id)
        client_id = connection.extra_dejson.get('client_id')
        client_secret = connection.extra_dejson.get('client_secret')
        developer_token = connection.extra_dejson.get('developer_token')
        refresh_token = connection.extra_dejson.get('refresh_token')

        authorization_data = AuthorizationData(developer_token=developer_token)
        authorization_data.authentication = OAuthDesktopMobileAuthCodeGrant(client_id=client_id)
        authorization_data.authentication.client_secret = client_secret
        authorization_data.authentication.request_oauth_tokens_by_refresh_token(refresh_token)
        return authorization_data

    def get_account_ids(self, authorization_data: AuthorizationData) -> List[int]:
        customer_service = ServiceClient(
            service='CustomerManagementService',
            version=13,
            authorization_data=authorization_data
        )
        paging = customer_service.factory.create('ns5:Paging')
        paging.Index = 0
        paging.Size = 100 # TODO: fix later
        user = customer_service.GetUser(UserId=None).User
        predicates = {
            'Predicate': [
                {
                    'Field': 'UserId',
                    'Operator': 'Equals',
                    'Value': user.Id,
                },
            ]
        }
        accounts = customer_service.SearchAccounts(
            PageInfo=paging,
            Predicates=predicates
        )
        account_ids = ([x.Id for x in accounts['AdvertiserAccount']])
        logging.info('Found accounts: %s', account_ids)
        return account_ids

    def execute(self, context: Any):
        Path("temp").mkdir(exist_ok=True)
        authorization_data = self.authorize()
        account_ids = self.get_account_ids(authorization_data)
        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data
        )

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data
        )

        report_request = reporting_service.factory.create('DestinationUrlPerformanceReportRequest')

        report_request.Aggregation = 'Daily'
        report_request.Format = 'Csv'
        report_request.ExcludeReportFooter = True
        report_request.ExcludeReportHeader = True
        report_request.ReturnOnlyCompleteData = False

        time = reporting_service.factory.create('ReportTime')

        start_date = reporting_service.factory.create('Date')
        start_date.Day = int(self.date_from.split('-')[2])
        start_date.Month = int(self.date_from.split('-')[1])
        start_date.Year = int(self.date_from.split('-')[0])

        end_date = reporting_service.factory.create('Date')
        end_date.Day = int(self.date_to.split('-')[2])
        end_date.Month = int(self.date_to.split('-')[1])
        end_date.Year = int(self.date_to.split('-')[0])

        time.CustomDateRangeEnd = end_date
        time.CustomDateRangeStart = start_date
        time.ReportTimeZone = 'GreenwichMeanTimeDublinEdinburghLisbonLondon'
        report_request.Time = time

        columns = reporting_service.factory.create('ArrayOfDestinationUrlPerformanceReportColumn')
        columns.DestinationUrlPerformanceReportColumn = self.columns
        report_request.Columns = columns

        report_request.ReportName = 'test'

        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': account_ids}
        scope.Campaigns = None
        report_request.Scope = scope

        logging.info(report_request)

        reporting_service_manager.download_report(ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory='temp/',
            result_file_name='download.csv',
            overwrite_result_file=True))

        df = pd.read_csv('temp/download.csv')
        shutil.rmtree('temp/')
        df.rename(columns=dict((column, stringcase.snakecase(column))for column in self.columns), inplace=True)
        logging.info(df)
        self.upload_csv_df(df)

