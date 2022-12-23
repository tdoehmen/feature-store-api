import pickle

import pyarrow
import pyarrow.flight
from pyarrow.flight import FlightServerError

from hsfs import client


class FlightClient:
    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = FlightClient()
        return cls.instance

    def __init__(self):
        self.client = client.get_instance()
        host_ip = "hopsworks0.logicalclocks.com"  # self.client._get_host_port_pair()[0]
        self.host_url = f"grpc+tls://{host_ip}:5005"
        (tls_root_certs, cert_chain, private_key) = self._extract_certs()
        self.connection = pyarrow.flight.FlightClient(
            location=self.host_url,
            tls_root_certs=tls_root_certs,
            cert_chain=cert_chain,
            private_key=private_key,
        )
        self._check_connection()
        self._register_certificates()

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

    def _handle_afs_errors(method):
        def afs_error_handler_wrapper(*args, **kw):
            try:
                return method(*args, **kw)
            except FlightServerError as e:
                message = str(e)
                if "Please register client certificates first." in message:
                    self = args[0]
                    self._register_certificates()
                    return method(*args, **kw)
                else:
                    raise

        return afs_error_handler_wrapper

    def _extract_certs(self):
        with open(self.client._get_ca_chain_path(), "rb") as f:
            tls_root_certs = f.read()
        with open(self.client._get_client_cert_path(), "r") as f:
            cert_chain = f.read()
        with open(self.client._get_client_key_path(), "r") as f:
            private_key = f.read()
        return tls_root_certs, cert_chain, private_key

    def _register_certificates(self):
        with open(self.client._get_jks_key_store_path(), "rb") as f:
            kstore = f.read()
        with open(self.client._get_jks_trust_store_path(), "rb") as f:
            tstore = f.read()
        cert_key = self.client._cert_key
        certificates_pickled = pickle.dumps(
            (kstore, tstore, cert_key)
        )  # TODO dump kstore, tstore, priv-key
        certificates_pickled_buf = pyarrow.py_buffer(certificates_pickled)
        action = pyarrow.flight.Action(
            "register-client-certificates", certificates_pickled_buf
        )
        try:
            self.connection.do_action(action)
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    @_handle_afs_errors
    def get_feature_group(self, feature_group):
        fg_name = f"{feature_group.feature_store_name.replace('_featurestore', '')}.{feature_group.name}_{feature_group.version}"
        descriptor = pyarrow.flight.FlightDescriptor.for_path(fg_name)
        return self._get_dataset(descriptor).to_pandas()

    @_handle_afs_errors
    def get_training_dataset(self, feature_view, version=1):
        training_dataset_path = self._path_from_feature_view(feature_view, version)
        descriptor = pyarrow.flight.FlightDescriptor.for_path(training_dataset_path)
        return self._get_dataset(descriptor).to_pandas()

    def _get_dataset(self, descriptor):
        info = self.connection.get_flight_info(descriptor)
        reader = self.connection.do_get(self._info_to_ticket(info))
        return reader.read_all()

    @_handle_afs_errors
    def create_training_dataset(self, feature_view, version=1):
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(
            feature_view, version
        )
        try:
            training_dataset_encoded = pickle.dumps(training_dataset_metadata)
            buf = pyarrow.py_buffer(training_dataset_encoded)
            action = pyarrow.flight.Action("create-training-dataset", buf)
            for result in self.connection.do_action(action):
                return result.body.to_pybytes()
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    def _path_from_feature_view(self, feature_view, version):
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(
            feature_view, version
        )
        path = (
            f"{training_dataset_metadata['featurestore_name']}_Training_Datasets/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}"
            f"_{training_dataset_metadata['tds_version']}/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}.parquet"
        )
        full_path = f"/Projects/{training_dataset_metadata['featurestore_name']}/{path}"
        return full_path

    def _training_dataset_metadata_from_feature_view(self, feature_view, version):
        training_dataset_metadata = {}
        training_dataset_metadata["name"] = feature_view.name
        training_dataset_metadata["version"] = f"{feature_view.version}"
        training_dataset_metadata["tds_version"] = f"{version}"
        query = feature_view.query
        query_string = (
            query.to_string()
            .replace(
                f"`{query._left_feature_group.feature_store_name}`.`",
                f"`{query._left_feature_group.feature_store_name.replace('_featurestore', '')}.",
            )
            .replace("`", '"')
        )
        tables, filters = self._get_tables_and_filters_from_query(query)
        training_dataset_metadata["query"] = {
            "query_string": query_string,
            "tables": tables,
            "filters": filters,
        }
        training_dataset_metadata[
            "featurestore_name"
        ] = query._left_feature_group.feature_store_name.replace("_featurestore", "")
        return training_dataset_metadata

    def _get_tables_and_filters_from_query(self, query):
        tables = {}
        fg = query._left_feature_group
        fg_name = f"{fg.feature_store_name.replace('_featurestore', '')}.{fg.name}_{fg.version}"
        tables[fg._id] = fg_name
        filters = query._filter

        for join in query._joins:
            join_tables, join_filters = self._get_tables_and_filters_from_query(
                join._query
            )
            tables.update(join_tables)
            filters = filters & join_filters if join_filters is not None else filters

        return tables, filters

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket

    def get_flights(self):
        return self.connection.list_flights()
