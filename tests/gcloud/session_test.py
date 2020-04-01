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

"""Tests for Cloud Spanner sessions."""

from tests.gcloud import emulator


class GCloudSessionTest(emulator.TestCase):

  def testListSessions(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud(
            'spanner', 'databases', 'create', 'test-database',
            '--instance=test-instance',
            '--ddl=CREATE TABLE mytable (a INT64, b INT64) PRIMARY KEY(a)'),
        self.JoinLines(''))
    # Create session
    self.CreateSession('test-instance', 'test-database')
    # List session
    # List Session returns approximateLastUseTime, createTime and name.
    self.assertGreater(
        self.RunGCloud(
            'spanner', 'databases', 'sessions', 'list',
            '--database=test-database', '--instance=test-instance').find(
                'instances/test-instance/databases/test-database/sessions/'), 0)

  def testDeleteSessions(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud(
            'spanner', 'databases', 'create', 'test-database',
            '--instance=test-instance',
            '--ddl=CREATE TABLE mytable (a INT64, b INT64) PRIMARY KEY(a)'),
        self.JoinLines(''))
    # Create session
    session_uri = self.CreateSession('test-instance', 'test-database')
    session_id = session_uri[session_uri.rfind('/') + 1:]
    # Delete session
    self.RunGCloud('spanner', 'databases', 'sessions', 'delete', session_id,
                   '--database=test-database', '--instance=test-instance')
    # List sessions to verify none exist.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'sessions', 'list',
                       '--database=test-database', '--instance=test-instance'),
        self.JoinLines(''))


if __name__ == '__main__':
  emulator.RunTests()
