#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import json
import humps
from typing import Optional, List, Union
from datetime import datetime, date

from hsfs import util, engine, feature_group
from hsfs.core import query_constructor_api, storage_connector_api, arrow_flight_client
from hsfs.constructor import join
from hsfs.constructor.filter import Filter, Logic
from hsfs.client.exceptions import FeatureStoreException


class Query:
    ERROR_MESSAGE_ALREADY_EXISTS = "Feature name {} already exists in query."
    ERROR_MESSAGE_CHANGE_PREFIX = (
        "Feature name {} already exists in query. Consider changing the prefix."
    )
    ERROR_MESSAGE_USE_PREFIX = (
        "Feature name {} already exists in query. Consider using a prefix."
    )
    ERROR_MESSAGE_FEATURE_NOT_UNIQUE = "Feature name {} is not unique."
    ERROR_MESSAGE_FEATURE_AMBIGUOUS = (
        "Feature name {} is ambiguous. Consider using a prefix."
    )
    ERROR_MESSAGE_FEATURE_NOT_FOUND = "Feature name {} not found in query."
    ERROR_MESSAGE_FEATURE_NOT_FOUND_FG = (
        "Feature name {} not found in any of the featuregroups in this query."
    )

    def __init__(
        self,
        left_feature_group,
        left_features,
        feature_store_name=None,
        feature_store_id=None,
        left_feature_group_start_time=None,
        left_feature_group_end_time=None,
        joins=None,
        filter=None,
    ):
        self._feature_store_name = feature_store_name
        self._feature_store_id = feature_store_id
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._left_feature_group_start_time = left_feature_group_start_time
        self._left_feature_group_end_time = left_feature_group_end_time
        self._joins = joins or []
        self._filter = Logic.from_response_json(filter)
        self._python_engine = True if engine.get_type() == "python" else False
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        if self._left_feature_group is not None and self._left_features is not None:
            self._populate_collections()

    def _feature_exists_in_query(self, feature_name, prefix=None):
        existing_features = self._query_features.get(feature_name, [])
        if any([feature[1] == prefix for feature in existing_features]):
            return True
        if prefix:
            name_with_prefix = f"{prefix}{feature_name}"
            existing_features = self._query_features.get(name_with_prefix, [])
            return any([feature[1] is None for feature in existing_features])

        return False

    def _add_to_collection(self, feat, prefix, featuregroup, query_feature=True):
        collection = (
            self._query_features if query_feature else self._featuregroup_features
        )
        feature_entry = (feat, prefix, featuregroup)
        collection[feat.name] = collection.get(feat.name, []) + [feature_entry]
        if prefix:
            name_with_prefix = f"{prefix}{feat.name}"
            collection[name_with_prefix] = collection.get(name_with_prefix, []) + [
                feature_entry
            ]
        if query_feature:
            self._feature_list.append(feature_entry)

    def _populate_collections(self):
        self._featuregroups = {self._left_feature_group}
        self._query_features = {}
        self._featuregroup_features = {}
        self._feature_list = []
        self._filters = self._filter

        for feat in self._left_features:
            if self._feature_exists_in_query(feat.name):
                raise FeatureStoreException(
                    Query.ERROR_MESSAGE_FEATURE_NOT_UNIQUE.format(feat.name)
                )
            self._add_to_collection(feat, None, self._left_feature_group)
        for feat in self._left_feature_group.features:
            self._add_to_collection(
                feat, None, self._left_feature_group, query_feature=False
            )
        for join_obj in self.joins:
            self._featuregroups.add(join_obj.query._left_feature_group)

            if self._filters is None:
                self._filters = join_obj.query._filter
            elif join_obj.query._filter is not None:
                self._filters = self._filters & join_obj.query._filter

            for feat in join_obj.query._left_features:
                self._add_to_collection(
                    feat, join_obj.prefix, join_obj.query._left_feature_group
                )
            for feat in join_obj.query._left_feature_group.features:
                self._add_to_collection(
                    feat,
                    join_obj.prefix,
                    join_obj.query._left_feature_group,
                    query_feature=False,
                )

    def _prep_read(self, online, read_options):
        fs_query = self._query_constructor_api.construct_query(self)
        sql_query = self._to_string(fs_query, online)

        if online:
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            online_conn = None

            if engine.get_instance().is_flyingduck_query_supported(self, read_options):
                sql_query = arrow_flight_client.get_instance().create_query_object(
                    self, sql_query
                )
            else:
                # Register on demand feature groups as temporary tables
                fs_query.register_external()

                # Register on hudi feature groups as temporary tables
                fs_query.register_hudi_tables(
                    self._feature_store_id,
                    self._feature_store_name,
                    read_options,
                )

        return sql_query, online_conn

    def read(
        self,
        online: Optional[bool] = False,
        dataframe_type: Optional[str] = "default",
        read_options: Optional[dict] = {},
    ):
        """Read the specified query into a DataFrame.

        It is possible to specify the storage (online/offline) to read from and the
        type of the output DataFrame (Spark, Pandas, Numpy, Python Lists).

        !!! warning "External Feature Group Engine Support"
            **Spark only**

            Reading a Query containing an External Feature Group directly into a
            Pandas Dataframe using Python/Pandas as Engine is not supported,
            however, you can use the Query API to create Feature Views/Training
            Data containing External Feature Groups.

        # Arguments
            online: Read from online storage. Defaults to `False`.
            dataframe_type: DataFrame type to return. Defaults to `"default"`.
            read_options: Dictionary of read options for Spark in spark engine.
                Only for python engine: Use key "hive_config" to pass a dictionary of hive or tez configurations.
                For example: `{"hive_config": {"hive.tez.cpu.vcores": 2, "tez.grouping.split-count": "3"}}`
                Defaults to `{}`.

        # Returns
            `DataFrame`: DataFrame depending on the chosen type.
        """
        if not read_options:
            read_options = {}

        sql_query, online_conn = self._prep_read(online, read_options)

        schema = None
        if (
            read_options
            and "pandas_types" in read_options
            and read_options["pandas_types"]
        ):
            schema = self._collect_features()
            if len(self.joins) > 0 or None in [f.type for f in schema]:
                raise ValueError(
                    "Pandas types casting only supported for feature_group.read()/query.select_all()"
                )

        return engine.get_instance().sql(
            sql_query,
            self._feature_store_name,
            online_conn,
            dataframe_type,
            read_options,
            schema,
        )

    def show(self, n: int, online: Optional[bool] = False):
        """Show the first N rows of the Query.

        !!! example "Show the first 10 rows"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())

            query.show(10)
            ```

        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
        read_options = {}
        sql_query, online_conn = self._prep_read(online, read_options)

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn, read_options
        )

    def join(
        self,
        sub_query: "Query",
        on: Optional[List[str]] = [],
        left_on: Optional[List[str]] = [],
        right_on: Optional[List[str]] = [],
        join_type: Optional[str] = "inner",
        prefix: Optional[str] = None,
    ):
        """Join Query with another Query.

        If no join keys are specified, Hopsworks will use the maximal matching subset of
        the primary keys of the feature groups you are joining.
        Joins of one level are supported, no nested joins.

        !!! example "Join two feature groups"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())
            ```

        !!! example "More complex join"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                    .join(fg2.select_all(), on=["date", "location_id"])
                    .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
            ```

        # Arguments
            sub_query: Right-hand side query to join.
            on: List of feature names to join on if they are available in both
                feature groups. Defaults to `[]`.
            left_on: List of feature names to join on from the left feature group of the
                join. Defaults to `[]`.
            right_on: List of feature names to join on from the right feature group of
                the join. Defaults to `[]`.
            join_type: Type of join to perform, can be `"inner"`, `"outer"`, `"left"` or
                `"right"`. Defaults to "inner".
            prefix: User provided prefix to avoid feature name clash. Prefix is applied to the right
                feature group of the query. Defaults to `None`.

        # Returns
            `Query`: A new Query object representing the join.
        """
        new_join = join.Join(
            sub_query, on, left_on, right_on, join_type.upper(), prefix
        )

        self._check_join(new_join)

        self._joins.append(new_join)

        self._populate_collections()

        return self

    def _check_join(self, join_obj):
        for feat in join_obj.query._left_features:
            prefix = join_obj.prefix
            if self._feature_exists_in_query(feat.name, prefix):
                name = f"{prefix}{feat.name}" if prefix else feat.name
                message = (
                    Query.ERROR_MESSAGE_CHANGE_PREFIX
                    if prefix
                    else Query.ERROR_MESSAGE_USE_PREFIX
                )
                raise FeatureStoreException(message.format(name))

    def as_of(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        exclude_until: Optional[Union[str, int, datetime, date]] = None,
    ):
        """Perform time travel on the given Query.

        This method returns a new Query object at the specified point in time. Optionally, commits before a
        specified point in time can be excluded from the query. The Query can then either be read into a Dataframe
        or used further to perform joins or construct a training dataset.

        !!! example "Reading features at a specific point in time:"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:34:11").read().show()
            ```

        !!! example "Reading commits incrementally between specified points in time:"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:34:11", exclude_until="2020-10-19 07:34:11").read().show()
            ```

        The first parameter is inclusive while the latter is exclusive.
        That means, in order to query a single commit, you need to query that commit time
        and exclude everything just before the commit.

        !!! example "Reading only the changes from a single commit"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:31:38", exclude_until="2020-10-20 07:31:37").read().show()
            ```

        When no wallclock_time is given, the latest state of features is returned. Optionally, commits before
        a specified point in time can still be excluded.

        !!! example "Reading the latest state of features, excluding commits before a specified point in time"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of(None, exclude_until="2020-10-20 07:31:38").read().show()
            ```

        Note that the interval will be applied to all joins in the query.
        If you want to query different intervals for different feature groups in
        the query, you have to apply them in a nested fashion:
        ```python
        query1.as_of(..., ...)
            .join(query2.as_of(..., ...))
        ```

        If instead you apply another `as_of` selection after the join, all
        joined feature groups will be queried with this interval:
        ```python
        query1.as_of(..., ...)  # as_of is not applied
            .join(query2.as_of(..., ...))  # as_of is not applied
            .as_of(..., ...)
        ```

        !!! warning
            This function only works for queries on feature groups with time_travel_format='HUDI'.

        !!! warning
            Excluding commits via exclude_until is only possible within the range of the Hudi active timeline.
            By default, Hudi keeps the last 20 to 30 commits in the active timeline.
            If you need to keep a longer active timeline, you can overwrite the options:
            `hoodie.keep.min.commits` and `hoodie.keep.max.commits`
            when calling the `insert()` method.

        # Arguments
            wallclock_time: Read data as of this point in time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.
            exclude_until: Exclude commits until this point in time. Strings should be formatted in one of the
                following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.

        # Returns
            `Query`. The query object with the applied time travel condition.
        """
        wallclock_timestamp = util.convert_event_time_to_timestamp(wallclock_time)

        exclude_until_timestamp = util.convert_event_time_to_timestamp(exclude_until)

        for _join in self._joins:
            _join.query.left_feature_group_end_time = wallclock_timestamp
            _join.query.left_feature_group_start_time = exclude_until_timestamp
        self.left_feature_group_end_time = wallclock_timestamp
        self.left_feature_group_start_time = exclude_until_timestamp
        return self

    def pull_changes(self, wallclock_start_time, wallclock_end_time):
        """
        !!! warning "Deprecated"
        `pull_changes` method is deprecated. Use
        `as_of(end_wallclock_time, exclude_until=start_wallclock_time) instead.
        """
        self.left_feature_group_start_time = util.convert_event_time_to_timestamp(
            wallclock_start_time
        )
        self.left_feature_group_end_time = util.convert_event_time_to_timestamp(
            wallclock_end_time
        )
        return self

    def filter(self, f: Union[Filter, Logic]):
        """Apply filter to the feature group.

        Selects all features and returns the resulting `Query` with the applied filter.
        !!! example
            ```python

            from hsfs.feature import Feature

            query.filter(Feature("weekly_sales") > 1000)
            query.filter(Feature("name").like("max%"))

            ```

        If you are planning to join the filtered feature group later on with another
        feature group, make sure to select the filtered feature explicitly from the
        respective feature group:
        ```python
        query.filter(fg.feature1 == 1).show(10)
        ```

        Composite filters require parenthesis:
        ```python
        query.filter((fg.feature1 == 1) | (fg.feature2 >= 2))
        ```

        !!! example "Filters are fully compatible with joins"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                .join(fg2.select_all(), on=["date", "location_id"])
                .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
                .filter((fg1.location_id == 10) | (fg1.location_id == 20))
            ```

        !!! example "Filters can be applied at any point of the query"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                .join(fg2.select_all().filter(fg2.avg_temp >= 22), on=["date", "location_id"])
                .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
                .filter(fg1.location_id == 10)
            ```

        # Arguments
            f: Filter object.

        # Returns
            `Query`. The query object with the applied filter.
        """
        self._check_filter(f)

        if self._filter is None:
            if isinstance(f, Filter):
                self._filter = Logic.Single(left_f=f)
            elif isinstance(f, Logic):
                self._filter = f
            else:
                raise TypeError(
                    "Expected type `Filter` or `Logic`, got `{}`".format(type(f))
                )
        elif self._filter is not None:
            self._filter = self._filter & f

        self._populate_collections()

        return self

    def _check_filter(self, f):
        if f is None:
            return

        if isinstance(f, Filter):
            self.get_featuregroup_by_feature(f._feature)
        elif isinstance(f, Logic):
            self._check_filter(f._left_f)
            self._check_filter(f._right_f)
            self._check_filter(f._left_l)
            self._check_filter(f._right_l)
        else:
            raise TypeError(
                "Expected type `Filter` or `Logic`, got `{}`".format(type(f))
            )

    def from_cache_feature_group_only(self):
        for _query in [join.query for join in self._joins] + [self]:
            if not isinstance(_query._left_feature_group, feature_group.FeatureGroup):
                return False
        return True

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "featureStoreName": self._feature_store_name,
            "featureStoreId": self._feature_store_id,
            "leftFeatureGroup": self._left_feature_group,
            "leftFeatures": self._left_features,
            "leftFeatureGroupStartTime": self._left_feature_group_start_time,
            "leftFeatureGroupEndTime": self._left_feature_group_end_time,
            "joins": self._joins,
            "filter": self._filter,
            "hiveEngine": self._python_engine,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        feature_group_json = json_decamelized["left_feature_group"]
        feature_group_obj = (
            feature_group.ExternalFeatureGroup.from_response_json(feature_group_json)
            if "storage_connector" in feature_group_json
            else feature_group.FeatureGroup.from_response_json(feature_group_json)
        )
        return cls(
            left_feature_group=feature_group_obj,
            left_features=json_decamelized["left_features"],
            feature_store_name=json_decamelized.get("feature_store_name", None),
            feature_store_id=json_decamelized.get("feature_store_id", None),
            left_feature_group_start_time=json_decamelized.get(
                "left_feature_group_start_time", None
            ),
            left_feature_group_end_time=json_decamelized.get(
                "left_feature_group_end_time", None
            ),
            joins=[
                join.Join.from_response_json(_join)
                for _join in json_decamelized.get("joins", [])
            ],
            filter=json_decamelized.get("filter", None),
        )

    @classmethod
    def _hopsworks_json(cls, json_dict):
        """
        This method is used by the Hopsworks helper job.
        It does not fully deserialize the message as the usecase is to
        send it straight back to Hopsworks to read the content of the query

        Args:
            json_dict (str): a json string containing a query object

        Returns:
            A partially deserialize query object
        """
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("hive_engine", None)
        new = cls(**json_decamelized)
        new._joins = humps.camelize(new._joins)
        return new

    def to_string(self, online=False):
        """
        !!! example
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())

            query.to_string()
            ```
        """
        fs_query = self._query_constructor_api.construct_query(self)

        return self._to_string(fs_query, online)

    def _to_string(self, fs_query, online=False):
        if online:
            return fs_query.query_online
        if fs_query.pit_query is not None:
            return fs_query.pit_query
        return fs_query.query

    def __str__(self):
        return self._query_constructor_api.construct_query(self)

    @property
    def left_feature_group_start_time(self):
        return self._left_feature_group_start_time

    @property
    def left_feature_group_end_time(self):
        return self._left_feature_group_end_time

    @left_feature_group_start_time.setter
    def left_feature_group_start_time(self, left_feature_group_start_time):
        self._left_feature_group_start_time = left_feature_group_start_time

    @left_feature_group_end_time.setter
    def left_feature_group_end_time(self, left_feature_group_end_time):
        self._left_feature_group_end_time = left_feature_group_end_time

    def append_feature(self, feature):
        """
        !!! example
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())

            query.append_feature('feature_name')
            ```
        """
        if self._feature_exists_in_query(feature.name):
            raise FeatureStoreException(
                Query.ERROR_MESSAGE_ALREADY_EXISTS.format(feature.name)
            )

        self._left_features.append(feature)

        self._populate_collections()

    def is_time_travel(self):
        return (
            self.left_feature_group_start_time
            or self.left_feature_group_end_time
            or any([_join.query.is_time_travel() for _join in self._joins])
        )

    @property
    def joins(self):
        return self._joins

    @property
    def featuregroups(self):
        return list(self._featuregroups)

    @property
    def filters(self):
        return self._filters

    @property
    def features(self):
        return [feat[0] for feat in self._feature_list]

    def get_featuregroup_by_feature(self, feature):
        fg_id = feature._feature_group_id

        if fg_id is None:
            # find featuregroup by feature name
            return self.get_feature_obj(
                feature.name, include_unselected=True, resolve_ambiguity=False
            )[2]
        else:
            # find featuregroup by featuregroup id
            for fg in self.featuregroups:
                if fg.id == fg_id:
                    return fg

        raise FeatureStoreException(
            Query.ERROR_MESSAGE_FEATURE_NOT_FOUND_FG.format(feature.name)
        )

    def get_feature_obj(
        self,
        feature_name: str,
        include_unselected=False,
        resolve_ambiguity=True,
    ):
        feature_lookup = (
            self._query_features
            if not include_unselected
            else self._featuregroup_features
        )
        if feature_name not in feature_lookup:
            raise FeatureStoreException(
                Query.ERROR_MESSAGE_FEATURE_NOT_FOUND.format(feature_name)
            )
        feats = feature_lookup[feature_name]

        # if only one feature with this name, return it
        if len(feats) == 1:
            return feats[0]

        # if there are multiple features with this name, return the one without prefix
        if resolve_ambiguity:
            for feat in feats:
                if feat[1] is None:
                    return feat

        # there are multiple features with this name and all have prefix, raise exception
        raise FeatureStoreException(
            Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS.format(feature_name)
        )

    def get_feature(self, feature_name):
        return self.get_feature_obj(feature_name)[0]

    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except FeatureStoreException:
            raise AttributeError(f"'Query' object has no attribute '{name}'. ")

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError(
                f"Expected type `str`, got `{type(name)}`. "
                "Features are accessible by name."
            )
        return self.get_feature(name)
