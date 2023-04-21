from common.hopsworks_client import HopsworksClient
from common.stop_watch import stopwatch
from locust import User, task, constant, events
from locust.runners import MasterRunner, LocalRunner


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Locust process init")

    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        environment.hopsworks_client = HopsworksClient(environment)

@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    print("Locust process quit")
    if isinstance(environment.runner, MasterRunner):
        # clean up
        environment.hopsworks_client.close()


#class FeatureGroupReadSmall(User):
#    wait_time = constant(0)
#    weight = 10
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fg_small = self.client.fs.get_feature_group("locust_small_fg1", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_feature_group_small(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fg_small.read(read_options={"use_spark": self.client.use_hive})


class FeatureGroupReadMedium(User):
    wait_time = constant(0)
    weight = 7

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fg_medium = self.client.fs.get_feature_group("locust_medium_fg1", version=1)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_group_medium(self):
        self.read()

    @stopwatch
    def read(self):
        self.fg_medium.read(read_options={"use_spark": self.client.use_hive})


#class FeatureGroupReadLarge(User):
#    wait_time = constant(0)
#    weight = 4
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fg_large = self.client.fs.get_feature_group("locust_very_large_fg1", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_feature_group_large(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fg_large.read(read_options={"use_spark": self.client.use_hive})


#class FeatureViewReadSmall(User):
#    wait_time = constant(0)
#    weight = 9
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fv_small = self.client.fs.get_feature_view("locust_small_fv", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_feature_view_small(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fv_small.get_batch_data(read_options={"use_spark": self.client.use_hive})


class FeatureViewReadMedium(User):
    wait_time = constant(0)
    weight = 6

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv_medium = self.client.fs.get_feature_view("locust_medium_fv", version=1)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view_medium(self):
        self.read()

    @stopwatch
    def read(self):
        self.fv_medium.get_batch_data(read_options={"use_spark": self.client.use_hive})


#class FeatureViewReadLarge(User):
#    wait_time = constant(0)
#    weight = 3
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fv_large = self.client.fs.get_feature_view("locust_very_large_fv", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_feature_view_small(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fv_large.get_batch_data(read_options={"use_spark": self.client.use_hive})


#class TrainingDatasetReadSmall(User):
#    wait_time = constant(0)
#    weight = 8
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fv_small = self.client.fs.get_feature_view("locust_small_fv", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_training_dataset_small(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fv_small.get_train_validation_test_split(1, read_options={"use_spark": self.client.use_hive})


#class TrainingDatasetReadMedium(User):
#    wait_time = constant(0)
#    weight = 5
#
#    def __init__(self, environment):
 #       super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fv_medium = self.client.fs.get_feature_view("locust_medium_fv", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_training_dataset_medium(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fv_medium.get_train_validation_test_split(1, read_options={"use_spark": self.client.use_hive})


class TrainingDatasetReadLarge(User):
    wait_time = constant(0)
    weight = 2

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv_large = self.client.fs.get_feature_view("locust_very_large_fv", version=1)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_training_dataset_large(self):
        self.read()

    @stopwatch
    def read(self):
        self.fv_large.get_train_validation_test_split(1, read_options={"use_spark": self.client.use_hive})
