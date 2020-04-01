::
 :: Copyright 2020 Google LLC
 ::
 :: Licensed under the Apache License, Version 2.0 (the "License");
 :: you may not use this file except in compliance with the License.
 :: You may obtain a copy of the License at
 ::
 ::      http://www.apache.org/licenses/LICENSE-2.0
 ::
 :: Unless required by applicable law or agreed to in writing, software
 :: distributed under the License is distributed on an "AS IS" BASIS,
 :: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 :: See the License for the specific language governing permissions and
 :: limitations under the License.
 ::
 ::

set PATH=C:\Program Files\Java\jdk1.7.0_75\bin;%PATH%


:: Code under repo is checked out to %KOKORO_ARTIFACTS_DIR%\git.
:: The final directory name in this path is determined by the scm name specified
:: in the job configuration
cd %KOKORO_ARTIFACTS_DIR%\git\cloud-spanner-emulator

call build.bat
exit %ERRORLEVEL%
