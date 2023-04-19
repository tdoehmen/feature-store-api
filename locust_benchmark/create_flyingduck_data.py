from common.hopsworks_client import HopsworksClient

import numpy as np
import pandas as pd

from hsfs.client.exceptions import RestAPIError
from hsfs.feature_view import FeatureView


class DataGenerator:
    def __init__(self, client):
        self.client = client
        self.connection = client.connection()
        self.fs = self.connection.get_feature_store()

    def setup_locust_flyinduck(self, small_rows, medium_rows, large_rows, schema_repetitions=1):
        self.create_fg_and_fvs("locust_small", small_rows, schema_repetitions)
        self.create_fg_and_fvs("locust_medium", medium_rows, schema_repetitions)
        self.create_fg_and_fvs("locust_large", large_rows, schema_repetitions)

    def create_fg_and_fvs(self, base_name, rows, schema_repetitions):
        fg1 = self.create_fg(base_name+"_fg1", rows, schema_repetitions)
        fg2 = self.create_fg(base_name+"_fg2", rows, schema_repetitions)
        query = fg1.select_all().join(fg2.select_except(["id"]), left_on=["id"], right_on=["id"])
        fv = self.create_fv(base_name+"_fv", query)

    def create_fg(self, name, rows, schema_repetitions):
        try:
            fg = self.fs.get_feature_group(name, version=1)
            fg.delete()
        except RestAPIError:
            pass

        df = self.generate_df(rows, schema_repetitions, name)
        locust_fg = self.fs.get_or_create_feature_group(
            name=name,
            version=1,
            primary_key=["id"],
            online_enabled=True,
            stream=True,
            event_time=name + "_ts_1_" + str(0)
        )
        locust_fg.insert(df)
        return locust_fg

    def create_fv(self, name, query=None):
        try:
            fv = self.fs.get_feature_view(name, version=1)
            fv.delete()
        except RestAPIError:
            pass
        except ValueError:
            FeatureView.clean(self.fs._id, name, 1)

        fv = self.fs.create_feature_view(
            name=name,
            query=query,
            version=1,
        )
        fv.create_train_validation_test_split(
            description = name + '_training_dataset',
            validation_size = 0.2,
            test_size = 0.3,
            coalesce = True,
        )
        return fv

    def generate_df(self, rows, schema_repetitions, name="rand"):
        data = {"id": range(0, rows)}
        df = pd.DataFrame.from_dict(data)
        for i in range(0, schema_repetitions):
            df[name + "_ts_1_" + str(i)] = np.datetime64('2021-01-01T00:00:00') + (np.random.rand(rows) * 60 * 60 * 24 * 365).astype('timedelta64[s]')
            df[name + "_ts_2_" + str(i)] = np.datetime64('2021-01-01T00:00:00') + (np.random.rand(rows) * 60 * 60 * 24 * 365).astype('timedelta64[s]')
            df[name + "_int_1_" + str(i)] = np.random.randint(0, 100000, rows)
            df[name + "_int_2_" + str(i)] = np.random.randint(0, 100000, rows)
            df[name + "_float_1_" + str(i)] = np.random.rand(rows)
            df[name + "_float_2_" + str(i)] = np.random.rand(rows)
            df[name + "_string_1_" + str(i)] = np.random.choice(['str' + str(i) for i in range(10)], rows)
            df[name + "_string_2_" + str(i)] = np.random.choice(['str' + str(i) for i in range(50)], rows)
            df[name + "_string_3_" + str(i)] = np.random.choice(['str' + str(i) for i in range(100)], rows)
            df[name + "_string_4_" + str(i)] = np.random.choice(['str' + str(i) for i in range(1000)], rows)

        return df


if __name__ == "__main__":
    hopsworks_client = HopsworksClient()
    generator = DataGenerator(hopsworks_client)
    generator.setup_locust_flyinduck(1000, 10000, 100000)
    hopsworks_client.close()