import random
import time

from common.hopsworks_client import HopsworksClient
from common.stop_watch import stopwatch
from locust import User, task, constant, events
from locust.runners import MasterRunner, LocalRunner


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Locust process init")

    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        # create feature view
        environment.hopsworks_client = HopsworksClient(environment)
        fg = environment.hopsworks_client.get_or_create_fg()
        environment.hopsworks_client.get_or_create_fv(fg)


@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    print("Locust process quit")
    if isinstance(environment.runner, MasterRunner):
        # clean up
        environment.hopsworks_client.get_or_create_fv(None).delete()
        environment.hopsworks_client.close()


class FeatureGroupRead(User):
    wait_time = constant(0)
    weight = 5
    # fixed_count = 1

    def __init__(self, environment):
        super().__init__(environment)
        self.env = environment
        self.client = HopsworksClient(environment)
        self.fg = self.client.get_or_create_fg()

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
        #time.sleep(0.2)
        self.fg.read(read_options={"use_spark":True})
        #self.fg.read()

