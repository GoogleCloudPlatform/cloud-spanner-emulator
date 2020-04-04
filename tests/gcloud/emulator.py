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

"""Test helpers for testing Cloud Spanner Emulator against gcloud."""

import json
import os
import os.path
import re
import signal
import subprocess
import time
import urllib.request
import builtins

import portpicker

from bazel_tools.tools.python.runfiles import runfiles
import unittest

# Get filepaths for binaries used in the emulator gcloud tests.
r = runfiles.Create()
GCLOUD_BINARY = os.environ.get('GCLOUD_DIR') + '/gcloud'
GATEWAY_BINARY = r.Rlocation('com_google_cloud_spanner_emulator/binaries/' + os.environ.get('GO_BINARY_DIR') + '/gateway_main')
EMULATOR_BINARY = r.Rlocation('com_google_cloud_spanner_emulator/binaries/emulator_main')


class TestCase(unittest.TestCase):
  """Base class for tests that need to test the emulator against gcloud."""

  def RunGCloud(self, *args):
    # Run gcloud with a temporary config dir so it does not mess up the default
    # config when run locally.
    env = {}
    if os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR'):
      env['CLOUDSDK_CONFIG'] = os.path.join(
          os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR'), '.config', 'gcloud')
    return subprocess.check_output(
        (GCLOUD_BINARY,) + args, env=env, universal_newlines=True).strip()

  def GCloudVersion(self):
    # Get the current installed version e.g., Google Cloud SDK 287.0.0.
    version_str = self.RunGCloud('version').splitlines()[0]
    try:
      version = re.search(r'Google Cloud SDK (.+?)\.\d\.\d$',
                          version_str).group(1)
      return int(version)
    except AttributeError:
      return 10000

  def JoinLines(self, *lines):
    return '\n'.join(lines)

  # Helper method to create sessions since gcloud CLI does not support creating
  # sessions. All sessions created by gcloud commands (when read or write are
  # executed) are also deleted after the command completes.
  def CreateSession(self, instance, database):
    url = 'http://localhost:{}/v1/projects/test-project/instances/{}/databases/{}/sessions'.format(
        self.http_port, instance, database)
    req = urllib.request.Request(url, method='POST')
    with urllib.request.urlopen(req) as response:
      r = response.read().decode('utf-8')
      data = json.loads(r)
      return data['name']

  # Returns the operation_uri for the long running operation asscociated with
  # creating an instance.
  def CreateInstance(self, instance_id):
    url = 'http://localhost:{}/v1/projects/test-project/instances'.format(
        self.http_port)
    body = {
        'instanceId': instance_id,
        'instance': {
            'displayName': instance_id,
            'nodeCount': '1',
            'config': 'projects/test-project/instanceConfigs/emulator-config'
        }
    }
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    with urllib.request.urlopen(req) as response:
      r = response.read().decode('utf-8')
      data = json.loads(r)
      return data['name']

  def setUp(self):
    super(TestCase, self).setUp()

    # Pick ports for the emulator.
    self.grpc_port = portpicker.pick_unused_port()
    self.http_port = portpicker.pick_unused_port()

    # Start the emulator subprocess.
    self.emulator = subprocess.Popen([
        GATEWAY_BINARY, '--grpc_binary', EMULATOR_BINARY, '--grpc_port',
        str(self.grpc_port), '--http_port',
        str(self.http_port)
    ])

    # Configure gcloud to talk to the emulator.
    self.RunGCloud('config', 'set', 'auth/disable_credentials', 'True')
    self.RunGCloud('config', 'set', 'project', 'test-project')
    self.RunGCloud('config', 'set', 'api_endpoint_overrides/spanner',
                   'http://localhost:{}/'.format(self.http_port))

    self.WaitForReady()

  def tearDown(self):
    super(TestCase, self).tearDown()

    # Stop the emulator subprocess. We send SIGINT instead of SIGTERM as the
    # gateway process only handles SIGINT (golang only considers SIGINT and
    # SIGKILL to be cross-platform, and SIGKILL cannot be captured). If we just
    # called .terminate() here, python sends a SIGTERM to the gateway process
    # which terminates the gateway process, but leaks the grpc server process
    # started by the gateway process.
    self.emulator.send_signal(signal.SIGINT)

    # Wait for emulator subprocess to close grpc and http gateway server and
    # release any network ports being used.
    self.emulator.wait()

  def WaitForReady(self):
    url = 'http://localhost:{}/v1/projects/test-project/instanceConfigs'.format(
        self.http_port)
    req = urllib.request.Request(url, method='GET')

    # Wait for up to 15 seconds for emulator http server to be up.
    max_retries = 15
    num_retries = 0
    while num_retries < max_retries:
      try:
        with urllib.request.urlopen(req) as response:
          response.read().decode('utf-8')
          return
      except urllib.error.URLError as e:
        if isinstance(e.reason, builtins.ConnectionRefusedError):
          # Sleep for 1 second if the server isn't up.
          num_retries += 1
          if num_retries >= max_retries:
            raise e
          time.sleep(1.0)
          continue
        else:
          raise Exception(e.file.read().decode())


def RunTests():
  unittest.main()
