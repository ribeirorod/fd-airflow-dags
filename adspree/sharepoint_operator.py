import abc
import logging

import pandas as pd
from airflow.models import Connection
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

from gcs import GCSBaseOperator


class SharepointToGCSOperator(GCSBaseOperator, metaclass=abc.ABCMeta):

    template_fields = ["bucket", "object_name"]

    def __init__(self, *, connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.ctx = None

    @abc.abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

    def execute(self, context):
        connection = Connection.get_connection_from_secrets(self.connection_id)
        user_credentials = UserCredential(connection.login, connection.password)
        self.ctx = ClientContext(f'{connection.schema}://{connection.host}/sites/Adspree').with_credentials(user_credentials)

        df = self.get_data()
        logging.info("Created dataframe with shape %s", df.shape)
        logging.info(df)
        self.upload_csv_df(df)


class SharepointRevenueToGCSOperator(SharepointToGCSOperator):
    select_fields = ['*', 'Author/Title', 'Product/Title', 'Editor/Title', 'Advertiser/Title', 'Cost_x0020_Center/Title', 'Account_x0020_Manager/Title']
    expand_fields = ['Author', 'Product', 'Editor', 'Advertiser', 'Cost_x0020_Center', 'Account_x0020_Manager']

    def transform_row(self, r):
        row = {
            'id': r.get_property('Id'),
            'year': int(r.get_property('Year')),
            'month': int(r.get_property('Month')),
            'booking_period': r.get_property('Date'),
            'cost_center_id': r.get_property('Cost_x0020_CenterId'),
            'cost_center_name': r.get_property('Cost_x0020_Center').get('Title') if r.get_property('Cost_x0020_CenterId') is not None else None,
            'account_manager_id': r.get_property('Account_x0020_ManagerId'),
            'account_manager_name': r.get_property('Account_x0020_Manager').get('Title') if r.get_property('Account_x0020_ManagerId') is not None else None,
            'advertiser_id': r.get_property('AdvertiserId'),
            'advertiser_name': r.get_property('Advertiser').get('Title') if r.get_property('AdvertiserId') is not None else None,
            'product_id': r.get_property('ProductId'),
            'product_name': r.get_property('Product').get('Title') if r.get_property('ProductId') is not None else None,
            'status': r.get_property('Status'),
            'revenue': r.get_property('Revenue'),
            'revenue_ist': r.get_property('Revenue_x0028_Ist_x0029_'),
            'booking_period_date': r.get_property('datetesting'),
            'comment': r.get_property('Comment'),
            'author_id': r.get_property('AuthorId'),
            'author_name': r.get_property('Author').get('Title') if r.get_property('AuthorId') is not None else None,
        }
        return row

    def get_data(self) -> pd.DataFrame:
        result = self.ctx.web.lists.get_by_title('EOM Data Entry').items.get().select(self.select_fields).expand(self.expand_fields).execute_query()
        df = pd.DataFrame([self.transform_row(r) for r in result])
        print(df)

        return df


class SharepointCostToGCSOperator(SharepointToGCSOperator):

    select_fields = ['*', 'Author/Title', 'Product/Title', 'Editor/Title', 'Advertiser/Title', 'Publisher/Title', 'Cost_x0020_Center/Title', 'Account_x0020_Manager/Title']
    expand_fields = ['Author', 'Product', 'Editor', 'Advertiser', 'Publisher', 'Cost_x0020_Center', 'Account_x0020_Manager']
    int_columns = ['id', 'year', 'month', 'cost_center_id', 'account_manager_id', 'advertiser_id', 'publisher_id', 'product_id', 'author_id']

    def transform_row(self, r):
        row = {
            'id': r.get_property('Id'),
            'year': int(r.get_property('Year')),
            'month': int(r.get_property('Month')),
            'booking_period': r.get_property('Date'),
            'cost_center_id': r.get_property('Cost_x0020_CenterId'),
            'cost_center_name': r.get_property('Cost_x0020_Center').get('Title') if r.get_property('Cost_x0020_CenterId') is not None else None,
            'account_manager_id': r.get_property('Account_x0020_ManagerId'),
            'account_manager_name': r.get_property('Account_x0020_Manager').get('Title') if r.get_property('Account_x0020_ManagerId') is not None else None,
            'advertiser_id': int(r.get_property('AdvertiserId')) if r.get_property('AdvertiserId') is not None else None,
            'advertiser_name': r.get_property('Advertiser').get('Title') if r.get_property('AdvertiserId') is not None else None,
            'publisher_id': r.get_property('PublisherId'),
            'publisher_name': r.get_property('Publisher').get('Title') if r.get_property('PublisherId') is not None else None,
            'product_id': r.get_property('ProductId'),
            'product_name': r.get_property('Product').get('Title') if r.get_property('ProductId') is not None else None,
            'status': r.get_property('Status'),
            'cost': r.get_property('Revenue'),
            'cost_ist': r.get_property('Cost_x0028_Ist_x0029_'),
            'booking_period_date': r.get_property('datetesting'),
            'comment': r.get_property('Comment'),
            'author_id': r.get_property('AuthorId'),
            'author_name': r.get_property('Author').get('Title') if r.get_property('AuthorId') is not None else None,
        }
        return row

    def get_data(self) -> pd.DataFrame:
        result = self.ctx.web.lists.get_by_title('FC Cost').items.get().select(self.select_fields).expand(self.expand_fields).execute_query()
        df = pd.DataFrame([self.transform_row(r) for r in result])
        for int_column in self.int_columns:
            df[int_column] = df[int_column].astype('Int64')
        print(df)

        return df
