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

"""Tests for Cloud Spanner DatabaseAdmin api."""

from tests.gcloud import emulator


class GCloudDatabaseAdminTest(emulator.TestCase):

  def testCreateDatabase(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'create', 'test-database',
                       '--instance=test-instance'), self.JoinLines(''))
    # Describe the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'describe', 'test-database',
                       '--instance=test-instance'),
        self.JoinLines(
            'name: projects/test-project/instances/test-instance/'
            'databases/test-database', 'state: READY'))

  def testListsEmptyDatabases(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # List the databases.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'list',
                       '--instance=test-instance'), self.JoinLines(''))

  def testListDatabases(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'create', 'test-database',
                       '--instance=test-instance'), self.JoinLines(''))
    # List the databases.
    # TODO : Remove version check after GCloud version is updated.
    if self.GCloudVersion() < 328:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'list',
                         '--instance=test-instance'),
          self.JoinLines(
              'NAME           STATE',
              'test-database  READY'))
    else:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'list',
                         '--instance=test-instance'),
          self.JoinLines(
              'NAME           STATE  VERSION_RETENTION_PERIOD  EARLIEST_VERSION_TIME  KMS_KEY_NAME',
              'test-database  READY'))

  def testDeleteDatabase(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'create', 'test-database',
                       '--instance=test-instance'), self.JoinLines(''))
    # Delete the database.
    # use --quiet to disable the interactive command prompt.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'delete',
                       'test-database', '--instance=test-instance', '--quiet'),
        self.JoinLines(''))

  def testCreateDatabaseWithDDL(self):
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
    # Describe the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'describe', 'test-database',
                       '--instance=test-instance'),
        self.JoinLines(
            'name: projects/test-project/instances/test-instance/'
            'databases/test-database', 'state: READY'))

    # TODO : Remove version check after GCloud version is updated.
    if self.GCloudVersion() <= 287:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'ddl', 'describe',
                         'test-database', '--instance=test-instance'),
          self.JoinLines(
              # pyformat: disable
              '--- |-',
              '  CREATE TABLE mytable (',
              '    a INT64,',
              '    b INT64,',
              '  ) PRIMARY KEY(a)'
              # pyformat: enable
          ))
    else:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'ddl', 'describe',
                         'test-database', '--instance=test-instance'),
          self.JoinLines(
              # pyformat: disable
              'CREATE TABLE mytable (',
              '  a INT64,',
              '  b INT64,',
              ') PRIMARY KEY(a);'
              # pyformat: enable
          ))

  def testCreateDatabaseAndGetDatabaseDDL(self):
    # Create an instance.
    self.RunGCloud('spanner', 'instances', 'create', 'test-instance',
                   '--config=emulator-config', '--description=Test Instance',
                   '--nodes', '3')
    # Create the database.
    self.assertEqual(
        self.RunGCloud(
            'spanner', 'databases', 'create', 'test-database',
            '--instance=test-instance',
            '--ddl=CREATE TABLE mytable (a INT64, b INT64, c STRING(256), d TIMESTAMP OPTIONS(allow_commit_timestamp=true)) PRIMARY KEY(a, b)'
        ), self.JoinLines(''))
    # Describe the database.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'describe', 'test-database',
                       '--instance=test-instance'),
        self.JoinLines(
            'name: projects/test-project/instances/test-instance/'
            'databases/test-database', 'state: READY'))

    # TODO : Remove version check after GCloud version is updated.
    if self.GCloudVersion() <= 287:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'ddl', 'describe',
                         'test-database', '--instance=test-instance'),
          self.JoinLines(
              # pyformat: disable
              '--- |-',
              '  CREATE TABLE mytable (',
              '    a INT64,',
              '    b INT64,',
              '    c STRING(256),',
              '    d TIMESTAMP OPTIONS (',
              '      allow_commit_timestamp = true',
              '    ),',
              '  ) PRIMARY KEY(a, b)'
              # pyformat: enable
          ))
    else:
      self.assertEqual(
          self.RunGCloud('spanner', 'databases', 'ddl', 'describe',
                         'test-database', '--instance=test-instance'),
          self.JoinLines(
              # pyformat: disable
              'CREATE TABLE mytable (',
              '  a INT64,',
              '  b INT64,',
              '  c STRING(256),',
              '  d TIMESTAMP OPTIONS (',
              '    allow_commit_timestamp = true',
              '  ),',
              ') PRIMARY KEY(a, b);'
              # pyformat: enable
          ))

    # TODO: Add a test that creates an index.
    # TODO: create tests for 'spanner databases ddl update'.


# Note: there are no tests for IAM because it is unsupported in the emulator.

if __name__ == '__main__':
  emulator.RunTests()
