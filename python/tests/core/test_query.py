#
#   Copyright 2022 Hopsworks AB
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

from hsfs import util
from hsfs.constructor import query
from datetime import datetime

class TestQuery:
    def test_as_of(self, mocker):
        patcher = mocker.patch("hsfs.feature_group.FeatureGroup")
        patcher = mocker.patch("hsfs.engine.get_type", return_value="python")
        FeatureGroupMock = patcher.start()
        fg = FeatureGroupMock()

        q = query.Query(
            left_feature_group=fg,
            left_features=fg._features,
            feature_store_name="Test",
            feature_store_id=12345
        )

        requested_start_time = "2022-01-01 16:45:12"
        requested_end_time = "2022-02-01 16:45:12"

        q.as_of(requested_end_time, exclude_until=requested_start_time)
        assert q.left_feature_group_start_time/1000 == datetime.strptime(requested_start_time,
                                                                    "%Y-%m-%d %H:%M:%S").timestamp()

        print(requested_end_time)
        print(q.left_feature_group_end_time)
        assert q.left_feature_group_end_time/1000 == datetime.strptime(requested_end_time,
                                                                    "%Y-%m-%d %H:%M:%S").timestamp()
