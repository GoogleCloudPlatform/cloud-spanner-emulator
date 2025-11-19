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

"""Generates the default_values.json used in the Spangres proc changelist."""

import os.path
from google.protobuf import text_format
import jinja2
from third_party.spanner_pg.codegen import postgresql_catalog_pb2
from pathlib import Path

GENERATED_CODE_HEADER = """
// GENERATED CODE
//
// Do not edit directly.
// See //third_party/spanner_pg/codegen/gen_default_values_embed.py for
// details.
//
"""

CATALOG_FILEPATH = (
  "third_party/spanner_pg/codegen/emulator_postgresql_catalog.textproto"
)
TEMPLATE_FILENAME = "default_values_embed.h.jinja2"


def GenerateDefaultValuesEmbedHeader():
  """Generates default_values_embed.h file from postgresql catalog.

  This function parses the postgresql catalog textproto, converts it to json
  and renders a jinja2 template to generate the default_values_embed.h file.

  The generated file will contain default values for specific PostgreSQL
  signature (optional) arguments.
  """
  # Open textproto file
  catalog_proto = postgresql_catalog_pb2.CatalogProto()
  file_content = Path(CATALOG_FILEPATH).read_text()
  text_format.Parse(file_content, catalog_proto)

  default_values = []
  for function in catalog_proto.functions:
    for signature in function.signatures:
      if not signature.HasField("oid"):
        continue

      sig_default_values = []
      for argument in signature.arguments:
        if argument.HasField("default_value"):
          sig_default_values.append(argument.default_value)
      if sig_default_values:
        default_values.append(
            {"proc_oid": signature.oid, "default_values": sig_default_values}
        )

  default_values_json = default_values

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
      "default_values": default_values_json,
  }
  print(template.render(context))


def main():
  GenerateDefaultValuesEmbedHeader()


if __name__ == "__main__":
  main()
