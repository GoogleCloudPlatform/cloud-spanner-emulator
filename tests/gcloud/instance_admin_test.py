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

"""Tests for Cloud Spanner InstanceAdmin api."""

from tests.gcloud import emulator


class GCloudInstanceAdminTest(emulator.TestCase):

  def testListsInstanceConfigs(self):
    self.assertEqual(
        self.RunGCloud('spanner', 'instance-configs', 'list'),
        self.JoinLines('NAME             DISPLAY_NAME',
                       'emulator-config  Emulator Instance Config'))

  def testDescribeInstanceConfigs(self):
    self.assertEqual(
        self.RunGCloud('spanner', 'instance-configs', 'describe',
                       'emulator-config'),
        self.JoinLines(
            'displayName: Emulator Instance Config',
            'name: projects/test-project/instanceConfigs/emulator-config'
            ))

  def testListsEmptyInstances(self):
    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'list'), self.JoinLines(''))

  def testCreateInstance(self):
    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                       '--config=emulator-config',
                       '--description=Test Instance', '--nodes', '3'),
        self.JoinLines(''))

  def testListInstances(self):
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')

    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'list'),
        self.JoinLines(
            'NAME           DISPLAY_NAME   CONFIG           NODE_COUNT  STATE',
            'test-instance  Test Instance  emulator-config  3           READY'))

  def testDescribeInstance(self):
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')

    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'describe', 'test-instance'),
        self.JoinLines(
            'config: projects/test-project/instanceConfigs/emulator-config',
            'displayName: Test Instance',
            'name: projects/test-project/instances/test-instance',
            'nodeCount: 3',
            'state: READY'))

  def testDeleteInstance(self):
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')

    # use --quiet to disable the interactive command prompt.
    self.assertEqual(
        self.RunGCloud('spanner', 'instances', 'delete', 'test-instance',
                       '--quiet'), self.JoinLines(''))


if __name__ == '__main__':
  emulator.RunTests()
