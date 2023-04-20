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


class FeatureViewReadLarge(User):
    wait_time = constant(0)
    weight = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fv_very_large = self.client.fs.get_feature_view("locust_very_large_fv", version=1)

    def on_start(self):
        print("Init user")

    def on_stop(self):
        print("Closing user")
        self.client.close()

    @task
    def read_feature_view_small(self):
        self.read()

    @stopwatch
    def read(self):
        self.fv_very_large.get_batch_data(read_options={"use_spark": self.client.use_hive})


#class TrainingDatasetReadLarge(User):
#    wait_time = constant(0)
#    weight = 1
#
#    def __init__(self, environment):
#        super().__init__(environment)
#        self.env = environment
#        self.client = HopsworksClient(environment)
#        self.fv_very_large = self.client.fs.get_feature_view("locust_very_large_fv", version=1)
#
#    def on_start(self):
#        print("Init user")
#
#    def on_stop(self):
#        print("Closing user")
#        self.client.close()
#
#    @task
#    def read_training_dataset_large(self):
#        self.read()
#
#    @stopwatch
#    def read(self):
#        self.fv_very_large.get_train_validation_test_split(1, read_options={"use_spark": self.client.use_hive})
