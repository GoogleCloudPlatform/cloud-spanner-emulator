#
# PostgreSQL is released under the PostgreSQL License, a liberal Open Source
# license, similar to the BSD or MIT licenses.
#
# PostgreSQL Database Management System
# (formerly known as Postgres, then as Postgres95)
#
# Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
#
# Portions Copyright © 1994, The Regents of the University of California
#
# Portions Copyright 2023 Google LLC
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose, without fee, and without a written agreement
# is hereby granted, provided that the above copyright notice and this
# paragraph and the following two paragraphs appear in all copies.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#
# THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
# "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
# MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#------------------------------------------------------------------------------

"""Generates the pg_proc.dat file used to construct the Spangres bootstrap catalog."""

import os.path
from google.protobuf import text_format
import jinja2
from third_party.spanner_pg.codegen import postgresql_catalog_pb2
from pathlib import Path

GENERATED_CODE_HEADER = """
# GENERATED CODE
#
# Do not edit directly.
# See //third_party/spanner_pg/codegen/gen_pg_proc.py for details.
#
"""

CATALOG_FILEPATH = (
  "third_party/spanner_pg/codegen/emulator_postgresql_catalog.textproto"
)
TEMPLATE_FILENAME = "pg_proc.dat.jinja2"
POSITIONAL_ONLY_ARG = 1


def _create_pg_proc_entry(signature, pg_name_path):
  """Creates a single pg_proc.dat entry from the signature and name path.

  Args:
    signature: The signature to create the entry from.
    pg_name_path: The PostgreSQL name path to create the entry from.

  Returns:
    A single entry to be added in pg_proc.dat
  """
  argtypes = [arg.type.name for arg in signature.arguments]
  has_named_arguments = any(
      arg.named_argument_kind != POSITIONAL_ONLY_ARG
      for arg in signature.arguments
  )
  argnames = []
  if has_named_arguments:
    argnames = [
        (arg.name if arg.named_argument_kind != POSITIONAL_ONLY_ARG else "")
        for arg in signature.arguments
    ]
  return {
      "oid": signature.oid,
      "proname": pg_name_path.name_path[1],
      "pronamespace": pg_name_path.name_path[0],
      "prolang": "spanner_internal",
      "prorettype": signature.return_type.name,
      "proargtypes": argtypes,
      "proargnames": argnames,
      "prosrc": pg_name_path.name_path[1],
  }


def generate_pg_proc_dat():
  """Generates pg_proc.dat file from postgresql catalog.

  This function parses the postgresql catalog textproto, converts it to json
  and renders a jinja2 template to generate the pg_proc.dat file.
  """
  # Open textproto file
  catalog_proto = postgresql_catalog_pb2.CatalogProto()
  file_content = Path(CATALOG_FILEPATH).read_text()
  text_format.Parse(file_content, catalog_proto)

  pg_proc_entries = []
  for function in catalog_proto.functions:
    for signature in function.signatures:
      # Signatures require an OID to have an entry in pg_proc.dat
      if not signature.HasField("oid"):
        continue
      for pg_name_path in signature.postgresql_name_paths:
        # We only need to output entries that are not in the default
        # namespace (pg)
        if pg_name_path.name_path[0] == "pg":
          continue

        pg_proc_entries.append(_create_pg_proc_entry(signature, pg_name_path))

  # Open jinja2 template file
  jinja_env = jinja2.Environment(
      undefined=jinja2.StrictUndefined,
      autoescape=False,
      loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
  )
  template = jinja_env.get_template(TEMPLATE_FILENAME)

  # Renders the template
  context = {
      "GENERATED_CODE_HEADER": GENERATED_CODE_HEADER,
      "pg_proc_entries": pg_proc_entries,
  }
  print(template.render(context))


def main():
  generate_pg_proc_dat()


if __name__ == "__main__":
  main()
