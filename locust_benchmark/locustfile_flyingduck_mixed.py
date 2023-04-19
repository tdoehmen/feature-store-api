from common.hopsworks_client import HopsworksClient
from common.stop_watch import stopwatch
from locust import User, task, constant, events
from locust.runners import MasterRunner, LocalRunner


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Locust process init")

    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        environment.hopsworks_client = HopsworksClient(environment)
        environment.fg_small = environment.hopsworks_client.fs.get_feature_group("locust_small_fg1", version=1)
        environment.fg_medium = environment.hopsworks_client.fs.get_feature_group("locust_medium_fg1", version=1)
        environment.fg_large = environment.hopsworks_client.fs.get_feature_group("locust_large_fg1", version=1)
        environment.fv_small = environment.hopsworks_client.fs.get_feature_view("locust_small_fv", version=1)
        environment.fg_medium = environment.hopsworks_client.fs.get_feature_view("locust_medium_fv", version=1)
        environment.fg_large = environment.hopsworks_client.fs.get_feature_view("locust_large_fv", version=1)

@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    print("Locust process quit")
    if isinstance(environment.runner, MasterRunner):
        # clean up
        environment.hopsworks_client.close()


class FeatureGroupReadSmall(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_group(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fg_small.read(read_options={"use_spark": self.env.use_hive})


class FeatureGroupReadMedium(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_group(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fg_medium.read(read_options={"use_spark": self.env.use_hive})


class FeatureGroupReadLarge(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_group(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fg_large.read(read_options={"use_spark": self.env.use_hive})


class FeatureViewReadSmall(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_small.get_batch_data(read_options={"use_spark": self.env.use_hive})


class FeatureViewReadMedium(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_medium.get_batch_data(read_options={"use_spark": self.env.use_hive})


class FeatureViewReadLarge(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_large.get_batch_data(read_options={"use_spark": self.env.use_hive})


class TrainingDatasetReadSmall(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_small.get_train_validation_test_split(1, read_options={"use_spark": self.env.use_hive})


class TrainingDatasetReadMedium(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_medium.get_train_validation_test_split(1, read_options={"use_spark": self.env.use_hive})


class TrainingDatasetReadLarge(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view(self):
        self.read()

    @stopwatch
    def read(self):
        self.env.fv_large.get_train_validation_test_split(1, read_options={"use_spark": self.env.use_hive})