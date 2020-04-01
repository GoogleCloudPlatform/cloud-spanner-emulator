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

"""Tests for Cloud Spanner gcloud command execute-sql and rows."""

from tests.gcloud import emulator


class GCloudReadWriteTest(emulator.TestCase):

  def testExecuteSql(self):
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
    # Perform a read.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'execute-sql', 'test-database',
                       '--instance=test-instance',
                       '--sql=SELECT * FROM mytable'), self.JoinLines(''))

  def testInsert(self):
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
    # Perform an insert.
    # Command outputs a commit timestamp that is hard to assert.
    self.RunGCloud('spanner', 'rows', 'insert', '--data=a=1,b=1',
                   '--table=mytable', '--database=test-database',
                   '--instance=test-instance')
    # Perform a read.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'execute-sql', 'test-database',
                       '--instance=test-instance',
                       '--sql=SELECT * FROM mytable'),
        self.JoinLines('a  b', '1  1'))

  def testUpdate(self):
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
    # Perform an insert.
    # Command outputs a commit timestamp that is hard to assert.
    self.RunGCloud('spanner', 'rows', 'insert', '--data=a=1,b=1',
                   '--table=mytable', '--database=test-database',
                   '--instance=test-instance')
    # Perform an update.
    # Command outputs a commit timestamp that is hard to assert.
    self.RunGCloud('spanner', 'rows', 'update', '--data=a=1,b=2',
                   '--table=mytable', '--database=test-database',
                   '--instance=test-instance')
    # Perform a read.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'execute-sql', 'test-database',
                       '--instance=test-instance',
                       '--sql=SELECT * FROM mytable'),
        self.JoinLines('a  b', '1  2'))

  def testDelete(self):
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
    # Perform an insert.
    # Command outputs a commit timestamp that is hard to assert.
    self.RunGCloud('spanner', 'rows', 'insert', '--data=a=1,b=1',
                   '--table=mytable', '--database=test-database',
                   '--instance=test-instance')
    # Perform a read to verify row exist.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'execute-sql', 'test-database',
                       '--instance=test-instance',
                       '--sql=SELECT * FROM mytable'),
        self.JoinLines('a  b', '1  1'))
    # Perform a delete.
    # Command outputs a commit timestamp that is hard to assert.
    self.RunGCloud('spanner', 'rows', 'delete', '--keys=1', '--table=mytable',
                   '--database=test-database', '--instance=test-instance')
    # Perform a read to verify row does not exist.
    self.assertEqual(
        self.RunGCloud('spanner', 'databases', 'execute-sql', 'test-database',
                       '--instance=test-instance',
                       '--sql=SELECT * FROM mytable'), self.JoinLines(''))

if __name__ == '__main__':
  emulator.RunTests()
