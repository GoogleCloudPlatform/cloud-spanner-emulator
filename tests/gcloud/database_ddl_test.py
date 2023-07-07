#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for Cloud Spanner gcloud command for ddl statements to CREATE/ALTER/DROP CHANGE STREAM."""

from tests.gcloud import emulator


class GCloudDatabaseDdlTest(emulator.TestCase):
  # TODO: Test returned strings from ddl.
  def testUpdateDDLChangeStream(self):
    # Create an instance.
    self.RunGCloud(
        'spanner',
        'instances',
        'create',
        'test-instance',
        '--config=emulator-config',
        '--description=Test Instance',
        '--nodes',
        '3',
    )
    # Create the database.
    self.assertEqual(
        self.RunGCloud(
            'spanner',
            'databases',
            'create',
            'test-database',
            '--instance=test-instance',
            '--ddl=CREATE TABLE mytable (a INT64, b INT64) PRIMARY KEY(a)',
        ),
        self.JoinLines(''),
    )
    # Perform an update to create a change stream.
    self.RunGCloud(
        'spanner',
        'databases',
        'ddl',
        'update',
        'test-database',
        '--instance=test-instance',
        '--ddl=CREATE CHANGE STREAM myChangeStream FOR ALL',
    )
    # Perform an update to alter a change stream's value capture type.
    self.RunGCloud(
        'spanner',
        'databases',
        'ddl',
        'update',
        'test-database',
        '--instance=test-instance',
        (
            '--ddl=ALTER CHANGE STREAM myChangeStream SET OPTIONS ('
            " value_capture_type = 'NEW_VALUES' )"
        ),
    )
    # Perform an update to alter a change stream's retention period.
    self.RunGCloud(
        'spanner',
        'databases',
        'ddl',
        'update',
        'test-database',
        '--instance=test-instance',
        (
            '--ddl=ALTER CHANGE STREAM myChangeStream SET OPTIONS ('
            " retention_period = '3d' )"
        ),
    )
    # Perform an update to suspend a change stream.
    self.RunGCloud(
        'spanner',
        'databases',
        'ddl',
        'update',
        'test-database',
        '--instance=test-instance',
        '--ddl=ALTER CHANGE STREAM myChangeStream DROP FOR ALL',
    )
    # Perform an update to drop a change stream.
    self.RunGCloud(
        'spanner',
        'databases',
        'ddl',
        'update',
        'test-database',
        '--instance=test-instance',
        '--ddl=DROP CHANGE STREAM myChangeStream',
    )


if __name__ == '__main__':
  emulator.RunTests()
