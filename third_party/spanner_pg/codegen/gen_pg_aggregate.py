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

"""Generates the pg_aggregate.dat file used to construct the Spangres bootstrap catalog."""

import os.path

from google.protobuf import text_format
import jinja2

from third_party.spanner_pg.codegen import postgresql_catalog_pb2
from pathlib import Path

GENERATED_CODE_HEADER = """
# GENERATED CODE
#
# Do not edit directly.
# See //third_party/spanner_pg/codegen/gen_pg_aggregate.py for details.
#
"""

CATALOG_FILEPATH = (
  "third_party/spanner_pg/codegen/emulator_postgresql_catalog.textproto"
)
TEMPLATE_FILENAME = "pg_aggregate.dat.jinja2"
MODE_SCALAR = 1
MODE_AGGREGATE = 2
MODE_ANALYTIC = 3


def _create_pg_aggregate_entry(signature, pg_name_path):
  """Creates a single pg_aggregate.dat entry from the signature and name path.

  Args:
    signature: The signature to create the entry from.
    pg_name_path: The PostgreSQL name path to create the entry from.

  Returns:
    A single entry to be added in pg_aggregate.dat
  """
  argtypes = [arg.type.name for arg in signature.arguments]
  return {
      "aggfnoid": f"{pg_name_path.name_path[1]}({','.join(argtypes)})",
      # TODO: Add in additional detail from the function catalog to
      # fill out the remaining fields - we do not encode these in the function
      # registry, for example AVG encodes the following:
      # { aggfnoid => 'avg(int8)', aggtransfn => 'int8_avg_accum',
      # aggfinalfn => 'numeric_poly_avg', aggcombinefn => 'int8_avg_combine',
      # aggserialfn => 'int8_avg_serialize',
      # aggdeserialfn => 'int8_avg_deserialize',
      # aggmtransfn => 'int8_avg_accum', aggminvtransfn => 'int8_avg_accum_inv',
      # aggmfinalfn => 'numeric_poly_avg', aggtranstype => 'internal',
      # aggtransspace => '48',
      # aggmtranstype => 'internal', aggmtransspace => '48' }
      # https://www.postgresql.org/docs/current/catalog-pg-aggregate.html
      # For now put a placeholder value for the required fields.
      "aggtransfn": "int8inc_any",
      "aggtranstype": "internal",
  }


def generate_pg_aggregate_dat():
  """Generates pg_aggregate.dat file from postgresql catalog.

  This function parses the postgresql catalog textproto, converts it to json
  and renders a jinja2 template to generate the pg_aggregate.dat file.
  """
  # Open textproto file
  catalog_proto = postgresql_catalog_pb2.CatalogProto()
  file_content = Path(CATALOG_FILEPATH).read_text()
  text_format.Parse(file_content, catalog_proto)

  pg_aggregate_entries = []
  for function in catalog_proto.functions:
    if function.mode != MODE_AGGREGATE:
      continue
    for signature in function.signatures:
      # Signatures require an OID to have an entry in pg_proc.dat
      # (and pg_aggregate.dat)
      if not signature.HasField("oid"):
        continue
      for pg_name_path in signature.postgresql_name_paths:
        # We only need to output entries that are not in the default
        # namespace (pg)
        if pg_name_path.name_path[0] == "pg":
          continue

        pg_aggregate_entries.append(
            _create_pg_aggregate_entry(signature, pg_name_path)
        )

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
      "pg_aggregate_entries": pg_aggregate_entries,
  }
  print(template.render(context))


def main():
  generate_pg_aggregate_dat()


if __name__ == "__main__":
  main()
