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

"""Tests for Cloud Spanner Operations api."""

from tests.gcloud import emulator


class GCloudOperationsTest(emulator.TestCase):

  def testDescribeOperation(self):
    # Create an instance to start a LRO.
    operation_uri = self.CreateInstance('test-instance')

    # Describe the operation.
    # Describe returns start & end time that are different across each response
    # therefore only matching a subset of the response.
    self.assertGreater(
        self.RunGCloud(
            'spanner', 'operations', 'describe', operation_uri,
            '--instance=test-instance').find(
                '  name: projects/test-project/instances/test-instance'), 0)

  def testListOperation(self):
    # Create an instance to start a LRO.
    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                       '--config=emulator-config',
                       '--description=Test Instance', '--nodes', '3'),
        self.JoinLines(''))

    # List the operation for given instance.
    self.assertEqual(
        self.RunGCloud('spanner', 'operations', 'list',
                       '--instance=test-instance'),
        self.JoinLines(
            'OPERATION_ID  STATEMENTS  DONE  @TYPE',
            '_auto0                    True  CreateInstanceMetadata'))


if __name__ == '__main__':
  emulator.RunTests()
