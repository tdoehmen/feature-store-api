import pickle

import pyarrow
import pyarrow.flight


class FlightClient:

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = FlightClient("grpc+tcp://localhost:5005")
        return cls.instance

    def __init__(self, host_url):
        self.connection = pyarrow.flight.FlightClient(host_url,**{})
        self._check_connection()

    def _check_connection(self):
        while True:
            try:
                action = pyarrow.flight.Action("healthcheck", b"")
                options = pyarrow.flight.FlightCallOptions(timeout=1)
                list(self.connection.do_action(action, options=options))
                break
            except pyarrow.flight.FlightTimedOutError as e:
                if "Deadline" in str(e):
                    print("Server is not ready, waiting...")

    @classmethod
    def pretty_print_flights(cls, flights):
        for flight in flights:
            descriptor = flight.descriptor
            if descriptor.descriptor_type == pyarrow.flight.DescriptorType.PATH:
                print("Path:", descriptor.path)
            elif descriptor.descriptor_type == pyarrow.flight.DescriptorType.CMD:
                print("Command:", descriptor.command)
            else:
                print("Unknown descriptor type")
            print('---')

    def get_feature_group(self, feature_group):
        fg_name = f"{feature_group.feature_store_name.replace('_featurestore','')}.{feature_group.name}_{feature_group.version}"
        descriptor = pyarrow.flight.FlightDescriptor.for_path(fg_name)
        return self._get_dataset(descriptor).to_pandas()

    def get_training_dataset(self, feature_view, version=1):
        training_dataset_path = self._path_from_feature_view(feature_view, version)
        descriptor = pyarrow.flight.FlightDescriptor.for_path(training_dataset_path)
        return self._get_dataset(descriptor).to_pandas()

    def _get_dataset(self, descriptor):
        info = self.connection.get_flight_info(descriptor)
        reader = self.connection.do_get(self._info_to_ticket(info))
        return reader.read_all()

    def create_training_dataset(self, feature_view, version=1):
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(feature_view, version)
        try:
            training_dataset_encoded = pickle.dumps(training_dataset_metadata)
            buf = pyarrow.py_buffer(training_dataset_encoded)
            action = pyarrow.flight.Action("create-training-dataset", buf)
            for result in self.connection.do_action(action):
                return result.body.to_pybytes()
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    def _path_from_feature_view(self, feature_view, version):
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(feature_view, version)
        path = f"{training_dataset_metadata['featurestore_name']}_Training_Datasets/{training_dataset_metadata['name']}_{training_dataset_metadata['version']}.parquet"
        full_path = f"/Projects/{training_dataset_metadata['featurestore_name']}/{path}"
        return full_path

    def _training_dataset_metadata_from_feature_view(self, feature_view, version):
        training_dataset_metadata = {}
        training_dataset_metadata["name"] = feature_view.name
        training_dataset_metadata["version"] = feature_view.version
        query = feature_view.query
        duckdb_query = query.to_string().replace(f"`{query._left_feature_group.feature_store_name}`.`",
                                                 f"`{query._left_feature_group.feature_store_name.replace('_featurestore','')}.") \
                                                 .replace("`","\"")
        training_dataset_metadata["query"] = duckdb_query
        training_dataset_metadata["feature_groups"] = self._get_tables_from_query(query)
        training_dataset_metadata["featurestore_name"] = query._left_feature_group.feature_store_name.replace("_featurestore","")
        return training_dataset_metadata

    def _get_tables_from_query(self, query):
        tables = []
        fg = query._left_feature_group
        fg_name = f"{fg.feature_store_name.replace('_featurestore','')}.{fg.name}_{fg.version}"
        fg_filter = query._filter
        tables.append((fg_name, None))

        for join in query._joins:
            join_tables = self._get_tables_from_query(join._query)
            tables.extend(join_tables)

        return tables

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket

    def get_flights(self):
        return self.connection.list_flights()