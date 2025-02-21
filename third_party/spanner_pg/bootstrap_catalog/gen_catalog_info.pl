#!/usr/bin/perl
#----------------------------------------------------------------------
#
# gen_catalog_info.pl
#  DERIVED FROM (with significant modifications)
# genbki.pl
#    Perl script that generates bootstrap_catalog_info.{h|c} from specially
#    formatted header files.  The files are used to populate the Spanner PG
#    bootstrap data.
#
# Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
#----------------------------------------------------------------------

use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use Carp;

use FindBin;
#use lib $FindBin::RealBin;
BEGIN  {
	use lib File::Spec->rel2abs(dirname(__FILE__) . "/../src/backend/catalog");
}

use Catalog;

my $output_path = '';
our @include_path;
my $include_path;
my $engine_catalog_path = '';

my $num_errors = 0;

# More args means more RAM reserved for the generated tables.
# Set this to the max number of args needed by any current PostgreSQL function.
# (We could compute this but then the script would need to be 2-pass.)
my $max_proc_args = 8;

GetOptions(
	'output:s'       => \$output_path,
	'include-path:s' => \$include_path,
	'engine-catalog-path:s' => \$engine_catalog_path) || usage();

# Sanity check arguments.
die "No input files.\n"                  unless @ARGV;
die "--include-path must be specified.\n" unless $include_path;

# Make sure paths end with a slash.
if ($output_path ne '' && substr($output_path, -1) ne '/')
{
	$output_path .= '/';
}
if (substr($include_path, -1) ne '/')
{
	$include_path .= '/';
}
if ($engine_catalog_path ne '' && substr($engine_catalog_path, -1) ne '/')
{
	$engine_catalog_path .= '/';
}

# Read all the files into internal data structures.
my @catnames;
my %catalogs;
my %catalog_data;
my @toast_decls;
my @index_decls;
my %oidcounts;

foreach my $header (@ARGV)
{
	$header =~ /(.+)\.h$/
	  or die "Input files need to be header files.\n";
	my $datfile = "$1.dat";
	my $engine_datfile = '';

	my $catalog = Catalog::ParseHeader($header);
	my $catname = $catalog->{catname};
	my $schema  = $catalog->{columns};

	if (defined $catname)
	{
		push @catnames, $catname;
		$catalogs{$catname} = $catalog;

		if ($engine_catalog_path ne '/') {
			$engine_datfile = $engine_catalog_path . $catname . '.dat';
		}
	}

	# While checking for duplicated OIDs, we ignore the pg_class OID and
	# rowtype OID of bootstrap catalogs, as those are expected to appear
	# in the initial data for pg_class and pg_type.  For regular catalogs,
	# include these OIDs.  (See also Catalog::FindAllOidsFromHeaders
	# if you change this logic.)
	if (!$catalog->{bootstrap})
	{
		$oidcounts{ $catalog->{relation_oid} }++
		  if ($catalog->{relation_oid});
		$oidcounts{ $catalog->{rowtype_oid} }++
		  if ($catalog->{rowtype_oid});
	}

	# Not all catalogs have a data file.
	if (-e $datfile)
	{
		parse_datfile($datfile, $schema, $catalog, \%catalog_data, $catname);
	}

	if ($engine_datfile ne '' && -e $engine_datfile)
	{
		parse_datfile($engine_datfile, $schema, $catalog, \%catalog_data, $catname);
	}

	# If the header file contained toast or index info, build BKI
	# commands for those, which we'll output later.
	foreach my $toast (@{ $catalog->{toasting} })
	{
		push @toast_decls,
		  sprintf "declare toast %s %s on %s\n",
		  $toast->{toast_oid}, $toast->{toast_index_oid},
		  $toast->{parent_table};
		$oidcounts{ $toast->{toast_oid} }++;
		$oidcounts{ $toast->{toast_index_oid} }++;
	}
	foreach my $index (@{ $catalog->{indexing} })
	{
		push @index_decls,
		  sprintf "declare %sindex %s %s %s\n",
		  $index->{is_unique} ? 'unique ' : '',
		  $index->{index_name}, $index->{index_oid},
		  $index->{index_decl};
		$oidcounts{ $index->{index_oid} }++;
	}
}

# Complain and exit if we found any duplicate OIDs.
# While duplicate OIDs would only cause a failure if they appear in
# the same catalog, our project policy is that manually assigned OIDs
# should be globally unique, to avoid confusion.
my $found = 0;
foreach my $oid (keys %oidcounts)
{
	next unless $oidcounts{$oid} > 1;
	print STDERR "Duplicate OIDs detected:\n" if !$found;
	print STDERR "$oid\n";
	$found++;
}
die "found $found duplicate OID(s) in catalog data\n" if $found;


# Oids not specified in the input files are automatically assigned,
# starting at FirstGenbkiObjectId.
my $FirstGenbkiObjectId =
  Catalog::FindDefinedSymbol('access/transam.h', $include_path,
	'FirstGenbkiObjectId');
my $GenbkiNextOid = $FirstGenbkiObjectId;


# Fetch some special data that we will substitute into the output file.
# CAUTION: be wary about what symbols you substitute into the file here!
# It's okay to substitute things that are expected to be really constant
# within a given Postgres release, such as fixed OIDs.  Do not substitute
# anything that could depend on platform or configuration.  (The right place
# to handle those sorts of things is in initdb.c's bootstrap_template1().)
my $C_COLLATION_OID =
  Catalog::FindDefinedSymbolFromData($catalog_data{pg_collation},
	'C_COLLATION_OID');

# Fill in pg_class.relnatts by looking at the referenced catalog's schema.
# This is ugly but there's no better place; Catalog::AddDefaultValues
# can't do it, for lack of easy access to the other catalog.
foreach my $row (@{ $catalog_data{pg_class} })
{
	$row->{relnatts} = scalar(@{ $catalogs{ $row->{relname} }->{columns} });
}


# Build lookup tables.

# access method OID lookup
my %amoids;
foreach my $row (@{ $catalog_data{pg_am} })
{
	$amoids{ $row->{amname} } = $row->{oid};
}

# role OID lookup
my %authidoids;
foreach my $row (@{ $catalog_data{pg_authid} })
{
	$authidoids{ $row->{rolname} } = $row->{oid};
}

# class (relation) OID lookup (note this only covers bootstrap catalogs!)
my %classoids;
foreach my $row (@{ $catalog_data{pg_class} })
{
	$classoids{ $row->{relname} } = $row->{oid};
}

# collation OID lookup
my %collationoids;
foreach my $row (@{ $catalog_data{pg_collation} })
{
	$collationoids{ $row->{collname} } = $row->{oid};
}

# language OID lookup
my %langoids;
foreach my $row (@{ $catalog_data{pg_language} })
{
	$langoids{ $row->{lanname} } = $row->{oid};
}

my %namespaceoids;
foreach my $row (@{ $catalog_data{pg_namespace} })
{
	if (not defined $row->{oid}) {
		$row->{oid} = $GenbkiNextOid;
		$GenbkiNextOid++;
	}
	$namespaceoids{ $row->{nspname} } = $row->{oid};
}

# opclass OID lookup
my %opcoids;
foreach my $row (@{ $catalog_data{pg_opclass} })
{
	# There is no unique name, so we need to combine access method
	# and opclass name.
	my $key = sprintf "%s/%s", $row->{opcmethod}, $row->{opcname};
	$opcoids{$key} = $row->{oid};
}

# operator OID lookup
my %operoids;
foreach my $row (@{ $catalog_data{pg_operator} })
{
	# There is no unique name, so we need to invent one that contains
	# the relevant type names.
	my $key = sprintf "%s(%s,%s)",
	  $row->{oprname}, $row->{oprleft}, $row->{oprright};
	$operoids{$key} = $row->{oid};
}

# opfamily OID lookup
my %opfoids;
foreach my $row (@{ $catalog_data{pg_opfamily} })
{
	# There is no unique name, so we need to combine access method
	# and opfamily name.
	my $key = sprintf "%s/%s", $row->{opfmethod}, $row->{opfname};
	$opfoids{$key} = $row->{oid};
}

# procedure OID lookup
my %procoids;
foreach my $row (@{ $catalog_data{pg_proc} })
{
	# Generate an entry under just the proname (corresponds to regproc lookup)
	my $prokey = $row->{proname};
	if (defined $procoids{$prokey})
	{
		$procoids{$prokey} = 'MULTIPLE';
	}
	else
	{
		$procoids{$prokey} = $row->{oid};
	}

	# Also generate an entry using proname(proargtypes).  This is not quite
	# identical to regprocedure lookup because we don't worry much about
	# special SQL names for types etc; we just use the names in the source
	# proargtypes field.  These *should* be unique, but do a multiplicity
	# check anyway.
	$prokey .= '(' . join(',', split(/\s+/, $row->{proargtypes})) . ')';
	if (defined $procoids{$prokey})
	{
		$procoids{$prokey} = 'MULTIPLE';
	}
	else
	{
		$procoids{$prokey} = $row->{oid};
	}
}

# tablespace OID lookup
my %tablespaceoids;
foreach my $row (@{ $catalog_data{pg_tablespace} })
{
	$tablespaceoids{ $row->{spcname} } = $row->{oid};
}

# text search configuration OID lookup
my %tsconfigoids;
foreach my $row (@{ $catalog_data{pg_ts_config} })
{
	$tsconfigoids{ $row->{cfgname} } = $row->{oid};
}

# text search dictionary OID lookup
my %tsdictoids;
foreach my $row (@{ $catalog_data{pg_ts_dict} })
{
	$tsdictoids{ $row->{dictname} } = $row->{oid};
}

# text search parser OID lookup
my %tsparseroids;
foreach my $row (@{ $catalog_data{pg_ts_parser} })
{
	$tsparseroids{ $row->{prsname} } = $row->{oid};
}

# text search template OID lookup
my %tstemplateoids;
foreach my $row (@{ $catalog_data{pg_ts_template} })
{
	$tstemplateoids{ $row->{tmplname} } = $row->{oid};
}

# type lookups
my %typeoids;
my %types;
foreach my $row (@{ $catalog_data{pg_type} })
{
	# for OID macro substitutions
	$typeoids{ $row->{typname} } = $row->{oid};

	# for pg_attribute copies of pg_type values
	$types{ $row->{typname} } = $row;
}

# Encoding identifier lookup.  This uses the same replacement machinery
# as for OIDs, but we have to dig the values out of pg_wchar.h.
my %encids;

my $encfile = $include_path . 'mb/pg_wchar.h';
open(my $ef, '<', $encfile) || die "$encfile: $!";

# We're parsing an enum, so start with 0 and increment
# every time we find an enum member.
my $encid             = 0;
my $collect_encodings = 0;
while (<$ef>)
{
	if (/typedef\s+enum\s+pg_enc/)
	{
		$collect_encodings = 1;
		next;
	}

	last if /_PG_LAST_ENCODING_/;

	if ($collect_encodings and /^\s+(PG_\w+)/)
	{
		$encids{$1} = $encid;
		$encid++;
	}
}

close $ef;

# Map lookup name to the corresponding hash table.
my %lookup_kind = (
	pg_am          => \%amoids,
	pg_authid      => \%authidoids,
	pg_class       => \%classoids,
	pg_collation   => \%collationoids,
	pg_language    => \%langoids,
	pg_namespace   => \%namespaceoids,
	pg_opclass     => \%opcoids,
	pg_operator    => \%operoids,
	pg_opfamily    => \%opfoids,
	pg_proc        => \%procoids,
	pg_tablespace  => \%tablespaceoids,
	pg_ts_config   => \%tsconfigoids,
	pg_ts_dict     => \%tsdictoids,
	pg_ts_parser   => \%tsparseroids,
	pg_ts_template => \%tstemplateoids,
	pg_type        => \%typeoids,
	encoding       => \%encids);


# Open temp files
my $tmpext  = ".tmp$$";
my $ccfile = $output_path . 'bootstrap_catalog_info.c';
open my $cc, '>', $ccfile . $tmpext
  or die "can't open $ccfile$tmpext: $!";
my $headerfile = $output_path . 'bootstrap_catalog_info.h';
open my $header, '>', $headerfile . $tmpext
  or die "can't open $headerfile$tmpext: $!";
my $textprotofile = $output_path . 'bootstrap_catalog_textproto.h';
open my $textproto, '>', $textprotofile . $tmpext
  or die "can't open $textprotofile$tmpext: $!";

# var for unused code
my $bki;

# vars to hold data needed for schemapg.h
my %schemapg_entries;
my @tables_needing_macros;

# vars to hold data needed for header.h
my %header_entries;
my %regprocoids;
our @types;

# Write file headers
print $header "#ifndef GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_INFO_H_\n";
print $header "#define GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_INFO_H_\n";
print $header "\n";
print $header "#ifdef __cplusplus\n";
print $header "extern \"C\" {\n";
print $header "#endif  // ifdef __cplusplus\n";
print $header "\n";
print $header "#include \"third_party/spanner_pg/postgres_includes/all.h\"\n";
print $header "\n";

print $textproto "#ifndef GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_TEXTPROTO_H_\n";
print $textproto "#define GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_TEXTPROTO_H_\n";
print $textproto "\n";
print $textproto "#include <string>\n";
print $textproto "#include <vector>\n";
print $textproto "\n";

print $cc "#include \"bootstrap_catalog_info.h\"\n";
print $cc "\n";
# Some constants used in the generated content
print $cc "#include <stdalign.h>\n";
print $cc "\n";

# Note: Carefully only using types and operations that can be evaluated
# at compile time and that don't require heap allocations,
# so this entire mess of data can be stored in the program's DATA area
# and mapped in from disk if/when it's needed.
print $cc "static const int ENCODING = -1; // TODO: Discover encoding\n";
print $cc "static const int SIZEOF_POINTER = sizeof(void*);\n";
print $cc "static const int ALIGNOF_POINTER = alignof(void*);\n";
print $cc "static const int LOCALE_PROVIDER = 'c';\n";
print $cc "\n";

# produce output, one catalog at a time
foreach my $catname (@catnames)
{
	# Header forward declarations,
	# enabling us to access compiled data for this catalog
 	my $catalog = $catalogs{$catname};
	print $header "#include \"catalog/${catname}.h\"\n";
	print $header "extern const FormData_${catname} ${catname}_data[];\n";
	print $header "extern const size_t ${catname}_data_size;\n";
	print $header "\n";
	print $cc "const FormData_${catname} ${catname}_data[] = {\n";
	print $textproto "const std::vector<std::string> ";
	print $textproto "${catname}_textproto_data = {\n";

	# 'bki' is PostgreSQL's name for some of the stuff we're building.
	# Doesn't make a lot of sense here, but the naming scheme is
	# shared with our common dependency `Catalog.pm`.
	# Keep it in a few places here to keep Perl happy.
	# (If you know enough Perl to do this more cleanly, feel free.)
	my %bki_attr;
	my %attnames;
	my $first = 1;

	foreach my $column (@{ $catalog->{columns} })
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};
		$bki_attr{$attname} = $column;
		# Build hash of column names for use later
		$attnames{$attname} = 1;
	}

	my $schema = $catalog->{columns};

	# Ordinary catalog with a data file
	foreach my $row (@{ $catalog_data{$catname} })
	{
		my %bki_values = %$row;

		# Complain about unrecognized keys; they are presumably misspelled
		foreach my $key (keys %bki_values)
		{
			next
			  if $key eq "oid_symbol"
			  || $key eq "array_type_oid"
			  || $key eq "descr"
			  || $key eq "autogenerated"
			  || $key eq "line_number";
			die sprintf "unrecognized field name \"%s\" in %s.dat line %s\n",
			  $key, $catname, $bki_values{line_number}
			  if (!exists($attnames{$key}));
		}

		# Perform required substitutions on fields
		foreach my $column (@$schema)
		{
			my $attname = $column->{name};
			my $atttype = $column->{type};

			# Assign oid if oid column exists and no explicit assignment in row
			if ($attname eq "oid" and not defined $bki_values{$attname})
			{
				$bki_values{$attname} = $GenbkiNextOid;
				$GenbkiNextOid++;
			}

			# Replace OID synonyms with OIDs per the appropriate lookup rule.
			#
			# If the column type is oidvector or _oid, we have to replace
			# each element of the array as per the lookup rule.
			if ($column->{lookup})
			{
				my $lookup = $lookup_kind{ $column->{lookup} };
				my $lookup_opt = $column->{lookup_opt};
				my @lookupnames;
				my @lookupoids;

				die "unrecognized BKI_LOOKUP type " . $column->{lookup}
				  if !defined($lookup);

				if ($atttype eq 'oidvector')
				{
					@lookupnames = split /\s+/, $bki_values{$attname};
					@lookupoids = lookup_oids($lookup, $catname, $attname, $lookup_opt,
					  \%bki_values, @lookupnames);
					$bki_values{$attname} = join(' ', @lookupoids);
				}
				elsif ($atttype eq '_oid')
				{
					if ($bki_values{$attname} ne '_null_')
					{
						$bki_values{$attname} =~ s/[{}]//g;
						@lookupnames = split /,/, $bki_values{$attname};
						@lookupoids =
						  lookup_oids($lookup, $catname, $attname,
							  $lookup_opt, \%bki_values, @lookupnames);
						$bki_values{$attname} = sprintf "{%s}",
						  join(',', @lookupoids);
					}
				}
				else
				{
					$lookupnames[0] = $bki_values{$attname};
					@lookupoids = lookup_oids($lookup, $catname, $attname, $lookup_opt,
					  \%bki_values, @lookupnames);
					$bki_values{$attname} = $lookupoids[0];
				}
			}
		}
		output_struct($cc, \%bki_values, $catalog, $catname);
		output_textproto($textproto, \%bki_values, $catalog, $catname);
	}

	print $cc "};\n";
	print $textproto "};\n";

	# Compute the array size in the compiled file and expose it as a symbol.
	#
	# Heads up for C++ developers on why we would do this:
	#
	# Forward-declared C arrays are just pointers; the size is not specified.
	# However, the compiler does know the size of the array that it just built.
	# So, capture that info as an exported variable so that callers can know
	# the array's size.
	# C++ users are encouraged to construct an std::span<> or equivalent
	# from the ${catname}_data and ${catname}_data_size arrays.
	# (std::span is a C++20 feature.  For devs using older C++,
	# Abseil has a similar class span absl::Span.)
	print $cc "const size_t ${catname}_data_size = sizeof(${catname}_data)/sizeof(${catname}_data[0]);\n";
	print $cc "\n";
	print $textproto "\n";
}

print $header "#ifdef __cplusplus\n";
print $header "}  // extern \"C\"\n";
print $header "namespace postgres_translator {\n";
print $header "constexpr Oid kHighestAssignedBootstrapOid = " . ($GenbkiNextOid - 1) . ";\n";
print $header "}  // namespace postgres_translator\n";
print $header "#endif  // __cplusplus\n";
print $header "\n";
print $header "#endif  // GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_INFO_H_\n";

print $textproto "#endif  // GENERATED__GEN_CATALOG_INFO_PL__BOOTSTRAP_CATALOG_TEXTPROTO_H_\n";

# We're done emitting data
close $cc;
close $header;

# Finally, rename the completed files into place.
Catalog::RenameTempFile($ccfile,    $tmpext);
Catalog::RenameTempFile($headerfile, $tmpext);
Catalog::RenameTempFile($textprotofile, $tmpext);

exit($num_errors != 0 ? 1 : 0);

#################### Subroutines ########################

sub parse_datfile
{
	my ($datfile, $schema, $catalog, $catalog_data, $catname) = @_;

	my $data = Catalog::ParseData($datfile, $schema, 0);

	foreach my $row (@$data)
	{
		push @{ $catalog_data{$catname} }, $row;
		# Generate entries for pg_description and pg_shdescription.
		if (defined $row->{descr})
		{
			my %descr = (
				objoid      => $row->{oid},
				classoid    => $catalog->{relation_oid},
				objsubid    => 0,
				description => $row->{descr});

			if ($catalog->{shared_relation})
			{
				delete $descr{objsubid};
				push @{ $catalog_data->{pg_shdescription} }, \%descr;
			}
			else
			{
				push @{ $catalog_data->{pg_description} }, \%descr;
			}
		}

		# Check for duplicated OIDs while we're at it.
		$oidcounts{ $row->{oid} }++ if defined $row->{oid};
	}
}	


# For each catalog marked as needing a schema macro, generate the
# per-user-attribute data to be incorporated into schemapg.h.  Also, for
# bootstrap catalogs, emit pg_attribute entries into the .bki file
# for both user and system attributes.
sub gen_pg_attribute_UNUSED
{
	my $schema = shift;

	my @attnames;
	foreach my $column (@$schema)
	{
		push @attnames, $column->{name};
	}

	foreach my $table_name (@catnames)
	{
		my $table = $catalogs{$table_name};

		# Currently, all bootstrap catalogs also need schemapg.h
		# entries, so skip if it isn't to be in schemapg.h.
		next if !$table->{schema_macro};

		$schemapg_entries{$table_name} = [];
		push @tables_needing_macros, $table_name;

		# Generate entries for user attributes.
		my $attnum       = 0;
		my $priornotnull = 1;
		foreach my $attr (@{ $table->{columns} })
		{
			$attnum++;
			my %row;
			$row{attnum}   = $attnum;
			$row{attrelid} = $table->{relation_oid};

			morph_row_for_pgattr(\%row, $schema, $attr, $priornotnull);
			$priornotnull &= ($row{attnotnull} eq 't');

			# If it's bootstrapped, put an entry in postgres.bki.
			print_bki_insert(\%row, $schema) if $table->{bootstrap};

			# Store schemapg entries for later.
			morph_row_for_schemapg(\%row, $schema);
			push @{ $schemapg_entries{$table_name} },
			  sprintf "{ %s }",
			  join(', ', grep { defined $_ } @row{@attnames});
		}

		# Generate entries for system attributes.
		# We only need postgres.bki entries, not schemapg.h entries.
		if ($table->{bootstrap})
		{
			$attnum = 0;
			my @SYS_ATTRS = (
				{ name => 'ctid',     type => 'tid' },
				{ name => 'xmin',     type => 'xid' },
				{ name => 'cmin',     type => 'cid' },
				{ name => 'xmax',     type => 'xid' },
				{ name => 'cmax',     type => 'cid' },
				{ name => 'tableoid', type => 'oid' });
			foreach my $attr (@SYS_ATTRS)
			{
				$attnum--;
				my %row;
				$row{attnum}        = $attnum;
				$row{attrelid}      = $table->{relation_oid};
				$row{attstattarget} = '0';

				morph_row_for_pgattr(\%row, $schema, $attr, 1);
				print_bki_insert(\%row, $schema);
			}
		}
	}
	return;
}

# Given $pgattr_schema (the pg_attribute schema for a catalog sufficient for
# AddDefaultValues), $attr (the description of a catalog row), and
# $priornotnull (whether all prior attributes in this catalog are not null),
# modify the $row hashref for print_bki_insert.  This includes setting data
# from the corresponding pg_type element and filling in any default values.
# Any value not handled here must be supplied by caller.
sub morph_row_for_pgattr
{
	my ($row, $pgattr_schema, $attr, $priornotnull) = @_;
	my $attname = $attr->{name};
	my $atttype = $attr->{type};

	$row->{attname} = $attname;

	# Copy the type data from pg_type, and add some type-dependent items
	my $type = $types{$atttype};

	$row->{atttypid}   = $type->{oid};
	$row->{attlen}     = $type->{typlen};
	$row->{attbyval}   = $type->{typbyval};
	$row->{attstorage} = $type->{typstorage};
	$row->{attalign}   = $type->{typalign};

	# set attndims if it's an array type
	$row->{attndims} = $type->{typcategory} eq 'A' ? '1' : '0';

	# collation-aware catalog columns must use C collation
	$row->{attcollation} =
	  $type->{typcollation} ne '0' ? $C_COLLATION_OID : 0;

	if (defined $attr->{forcenotnull})
	{
		$row->{attnotnull} = 't';
	}
	elsif (defined $attr->{forcenull})
	{
		$row->{attnotnull} = 'f';
	}
	elsif ($priornotnull)
	{

		# attnotnull will automatically be set if the type is
		# fixed-width and prior columns are all NOT NULL ---
		# compare DefineAttr in bootstrap.c. oidvector and
		# int2vector are also treated as not-nullable.
		$row->{attnotnull} =
		    $type->{typname} eq 'oidvector'  ? 't'
		  : $type->{typname} eq 'int2vector' ? 't'
		  : $type->{typlen} eq 'NAMEDATALEN' ? 't'
		  : $type->{typlen} > 0              ? 't'
		  :                                    'f';
	}
	else
	{
		$row->{attnotnull} = 'f';
	}

	Catalog::AddDefaultValues($row, $pgattr_schema, 'pg_attribute');
	return;
}

# Output one record of a FormData_* struct
sub output_struct
{
	my ($cc, $values, $catalog, $catname) = @_;

	print $cc "{";
	for my $i (0..$#{ $catalog->{columns} })
 	{
		my $column = $catalog->{columns}[$i];
		# Skip non-C columns
		if ($column->{is_varlen}) {
			next;
		}

 		my $attname = $column->{name};
		my $val = $values->{$attname};
		my $coltype = $column->{type};
		output_field($cc, $attname, $val, $coltype, $catname);
	}

	print $cc "},";
 	print $cc "\n";
}

sub output_textproto
{
	my($cc, $values, $catalog, $catname) = @_;
	print $cc "  \"";

	for my $i (0..$#{ $catalog->{columns} })
 	{
		my $column = $catalog->{columns}[$i];

 		my $attname = $column->{name};
		my $val = $values->{$attname};
		my $coltype = $column->{type};
		output_field_for_textproto($cc, $attname, $val, $coltype, $catname);
	}

	print $cc "\",\n";
}

# Output a single field for a textproto.
# The field looks like "fieldname: Value, ".
sub output_field_for_textproto
{
	my ($cc, $attname, $val, $coltype, $catname) = @_;
	if ($val eq "_null_")
	{
		# Textproto will parse absent values as null.
		return;
	}
	if (index($coltype, "_") != -1)
	{
		if ($val =~ /^{[\w, ]*}$/)
		{
			# Strip leading and trailing brackets
			$val = substr($val, 1, -1);
		}
		my @vals;
		if ($val =~ /^[\w,]*$/) {
			@vals = split(/,/, $val);
		} else {
			@vals = split(/, /, $val);
		}

		foreach my $element (@vals)
		{
			output_field_for_textproto(
				$cc, $attname, $element, substr($coltype, 1), $catname);
		}
		return;
	}
	elsif ($coltype eq "oidvector")
	{
		if ($val =~ /^"[0-9 ]*"$/)
		{
			# Strip leading and trailing quotes
			$val = substr($val, 1, -1);
		}
		my @vals = split(/ /, $val);

		foreach my $oid (@vals)
		{
			output_field_for_textproto($cc, $attname, $oid, "oid", $catname);
		}
		return;
	}

	print $cc "$attname: ";

	if ($coltype eq "regproc" && $val !~ /^[0-9]*$/)
	{
		# Regproc fields are cross references.
		# If $regprocoids (above) hasn't already resolved this reference to an
		# oid, omit it so we can still compile.
		# This probably means that the order of scanned #include files is
		# wrong.
		print $cc "InvalidOid /*$val*/";
	}
	elsif ($coltype =~ /^int.*/ && $val =~ /^"[0-9 ]*"$/)
	{
		# We seem to have some quoted numbers.  Not clear why.
		$val = substr($val, 1, -1);
		print $cc $val
	}
	# Format based on data type
	elsif ($coltype eq "name")
	{
		# Make sure value is wrapped as \"STRING\"
		# In the existing source, sometimes we have "STRING"
		# and sometimes we just have STRING
		if ($val !~ /^\{".*"\}$/)
		{
			$val = "\\\"$val\\\"";
		}
		print $cc $val;
	}
	elsif ($coltype eq "text")
	{
		print $cc "\\\"$val\\\"";
	}
	elsif ($coltype eq "char" && (length($val) == 0 || length($val) == 1 || substr($val, 0, 1) eq "\\"))
	{
		if (length($val) == 0)
		{
			# Represent the empty character as null.
			print $cc "\\\"\\\"";
		}
		else
		{
			# Quote single-character chars
			print $cc "\\\"$val\\\"";
		}
	}
	elsif ($coltype eq "bool")
	{
		# Convert SQL bool syntax to textproto bool syntax.
		# Pass enum/#define'd constants through.
		if ($val eq "t")
		{
			print $cc "true";
		}
		elsif ($val eq "f")
		{
			print $cc "false";
		}
		else
		{
			print $cc $val;
		}
	}
	else
	{
		# If we don't know what it is,
		# it's probably something that is represented equivalently
		# in SQL and in C.  Just pass it through.
		print $cc $val;
	}

	print $cc ", ";
}

# Output a single field for a FormData struct.
# The field looks like ".fieldname = Value, ".
# Most of the logic of this function is dedicated to figuring out how to
# properly render "Value" as a valid C constant.
sub output_field
{
	my ($cc, $attname, $val, $coltype, $catname) = @_;

	# Skip anything inside the CATALOG_VARLEN blocks in the struct
	# definitions.
	# Those blocks are never enabled when compiling C structs.
	# They sometimes don't even refer to valid C types.
	# The fields do exist in the PostgreSQL catalog, but we're just going
	# to ignore them for now.
	# Unfortunately, Postgres's handy catalog parser doesn't flag
	# CATALOG_VARLEN fields.  But they all use types that are identifiably
	# variable-length (and no other fields appear to do likewise), so
	# we identify them that way.
	if (index($coltype, "[]") != -1)
	{
		# Skip arrays; they are varlen fields so can't be represented
		# by the C structs that we're using here
		return;
	}
	elsif ($coltype eq "text")
	{
		# text colums count as arrays per the above,
		# and can't be represented by the C structs that we're using here
		return;
	}
	elsif ($coltype eq "timestamptz")
	{
		# Not sure why this is a variable-length type, but it seems to be one?
		# Likewise doesn't work.
		return;
	}
	elsif ($coltype eq "pg_node_tree")
	{
		# Definitely variable-size, so not supported.
		return;
	}
	elsif ($coltype eq "oidvector")
	{
		return;
	}

	print $cc ".$attname = ";

	if ($coltype eq "regproc" && $val !~ /^[0-9]*$/)
	{
		# Regproc fields are cross references.
		# If $regprocoids (above) hasn't already resolved this reference to an
		# oid, omit it so we can still compile.
		# This probably means that the order of scanned #include files is
		# wrong.
		print $cc "InvalidOid /*$val*/";
	}
	elsif ($coltype =~ /^int.*/ && $val =~ /^"[0-9 ]*"$/)
	{
		# We seem to have some quoted numbers.  Not clear why.
		$val = substr($val, 1, -1);
		print $cc $val
	}
	elsif ($val eq "_null_")
	{
		# Special-case catalog null to C null.
		print $cc "NULL";
	}
	# Format based on data type
	elsif ($coltype eq "name")
	{
		# Make sure value is wrapped as {"STRING"}
		# In the existing source, sometimes we have "STRING"
		# and sometimes we just have STRING
		if ($val !~ /^\{".*"\}$/)
		{
			if ($val !~ /^".*"$/)
			{
				$val = "\"$val\"";
			}
			$val = "{$val}";
		}
		print $cc $val;
	}
	elsif ($coltype eq "char" && (length($val) == 0 || length($val) == 1 || substr($val, 0, 1) eq "\\"))
	{
		if (length($val) == 0)
		{
			# Represent the empty character as nul.
			print $cc "'\\0'";
		}
		else
		{
			# Quote single-character chars
			print $cc "'$val'";
		}
	}
	elsif ($coltype eq "bool")
	{
		# Convert SQL bool syntax to C bool syntax.
		# Pass enum/#define'd constants through.
		if ($val eq "t")
		{
			print $cc "true";
		}
		elsif ($val eq "f")
		{
			print $cc "false";
		}
		else
		{
			print $cc $val;
		}
	}
	else
	{
		# If we don't know what it is,
		# it's probably something that is represented equivalently
		# in SQL and in C.  Just pass it through.
		print $cc $val;
	}

	print $cc ", ";
}

# Given a system catalog name and a reference to a key-value pair corresponding
# to the name and type of a column, generate a reference to a hash that
# represents a pg_attribute entry.  We must also be told whether preceding
# columns were all not-null.
sub emit_pgattr_row
 {
	my ($table_name, $attr, $priornotnull) = @_;
	my $attname = $attr->{name};
	my $atttype = $attr->{type};
	my %row;

	$row{attrelid} = $catalogs{$table_name}->{relation_oid};
	$row{attname}  = $attname;

	# Adjust type name for arrays: foo[] becomes _foo
	# so we can look it up in pg_type
	if ($atttype =~ /(.+)\[\]$/)
	{
		$atttype = '_' . $1;
	}

	# Copy the type data from pg_type, and add some type-dependent items
	foreach my $type (@types)
	{
		if (defined $type->{typname} && $type->{typname} eq $atttype)
		{
			$row{atttypid}   = $type->{oid};
			$row{attlen}     = $type->{typlen};
			$row{attbyval}   = $type->{typbyval};
			$row{attstorage} = $type->{typstorage};
			$row{attalign}   = $type->{typalign};

			# set attndims if it's an array type
			$row{attndims} = $type->{typcategory} eq 'A' ? '1' : '0';
			$row{attcollation} = $type->{typcollation};

			if (defined $attr->{forcenotnull})
			{
				$row{attnotnull} = 't';
			}
			elsif (defined $attr->{forcenull})
			{
				$row{attnotnull} = 'f';
			}
			elsif ($priornotnull)
			{

				# attnotnull will automatically be set if the type is
				# fixed-width and prior columns are all NOT NULL ---
				# compare DefineAttr in bootstrap.c. oidvector and
				# int2vector are also treated as not-nullable.
				$row{attnotnull} =
				    $type->{typname} eq 'oidvector'   ? 't'
				  : $type->{typname} eq 'int2vector'  ? 't'
				  : $type->{typlen}  eq 'NAMEDATALEN' ? 't'
				  : $type->{typlen} > 0 ? 't'
				  :                       'f';
			}
			else
			{
				$row{attnotnull} = 'f';
			}
			last;
		}
	}
 
	# Add in default values for pg_attribute
	my %PGATTR_DEFAULTS = (
		attcacheoff   => '-1',
		atttypmod     => '-1',
		atthasdef     => 'f',
		attidentity   => '',
		attisdropped  => 'f',
		attislocal    => 't',
		attinhcount   => '0',
		attacl        => '_null_',
		attoptions    => '_null_',
		attfdwoptions => '_null_');
	return { %PGATTR_DEFAULTS, %row };
}

# Write an entry to postgres.bki.
sub print_bki_insert_UNUSED
{
	my $row    = shift;
	my $schema = shift;

	my @bki_values;

	foreach my $column (@$schema)
	{
		my $attname   = $column->{name};
		my $atttype   = $column->{type};
		my $bki_value = $row->{$attname};

		# Fold backslash-zero to empty string if it's the entire string,
		# since that represents a NUL char in C code.
		$bki_value = '' if $bki_value eq '\0';

		# Handle single quotes by doubling them, and double quotes by
		# converting them to octal escapes, because that's what the
		# bootstrap scanner requires.  We do not process backslashes
		# specially; this allows escape-string-style backslash escapes
		# to be used in catalog data.
		$bki_value =~ s/'/''/g;
		$bki_value =~ s/"/\\042/g;

		# Quote value if needed.  We need not quote values that satisfy
		# the "id" pattern in bootscanner.l, currently "[-A-Za-z0-9_]+".
		$bki_value = sprintf(qq'"%s"', $bki_value)
		  if length($bki_value) == 0
		  or $bki_value =~ /[^-A-Za-z0-9_]/;

		push @bki_values, $bki_value;
	}
	printf $bki "insert ( %s )\n", join(' ', @bki_values);
	return;
}

# Given a row reference, modify it so that it becomes a valid entry for
# a catalog schema declaration in schemapg.h.
#
# The field values of a Schema_pg_xxx declaration are similar, but not
# quite identical, to the corresponding values in postgres.bki.
sub morph_row_for_schemapg
{
	my $row           = shift;
	my $pgattr_schema = shift;

	foreach my $column (@$pgattr_schema)
	{
		my $attname = $column->{name};
		my $atttype = $column->{type};

		# Some data types have special formatting rules.
		if ($atttype eq 'name')
		{
			# add {" ... "} quoting
			$row->{$attname} = sprintf(qq'{"%s"}', $row->{$attname});
		}
		elsif ($atttype eq 'char')
		{
			# Add single quotes
			$row->{$attname} = sprintf("'%s'", $row->{$attname});
		}

		# Expand booleans from 'f'/'t' to 'false'/'true'.
		# Some values might be other macros (eg FLOAT8PASSBYVAL),
		# don't change.
		elsif ($atttype eq 'bool')
		{
			$row->{$attname} = 'true'  if $row->{$attname} eq 't';
			$row->{$attname} = 'false' if $row->{$attname} eq 'f';
		}

		# We don't emit initializers for the variable length fields at all.
		# Only the fixed-size portions of the descriptors are ever used.
		delete $row->{$attname} if $column->{is_varlen};
	}
	return;
}

# Perform OID lookups on an array of OID names.
# If we don't have a unique value to substitute, warn and
# leave the entry unchanged.
# (A warning seems sufficient because the bootstrap backend will reject
# non-numeric values anyway.  So we might as well detect multiple problems
# within this genbki.pl run.)
sub lookup_oids
{
	my ($lookup, $catname, $attname, $lookup_opt, $bki_values, @lookupnames)
	  = @_;

	my @lookupoids;
	foreach my $lookupname (@lookupnames)
	{
		my $lookupoid = $lookup->{$lookupname};
		if (defined($lookupoid) and $lookupoid ne 'MULTIPLE')
		{
			push @lookupoids, $lookupoid;
		}
		else
		{
			push @lookupoids, $lookupname;
			if ($lookupname eq '-' or $lookupname eq '0')
			{
				if (!$lookup_opt)
				{
					Carp::confess sprintf
					  "invalid zero OID reference in %s.dat field %s line %s\n",
					  $catname, $attname, $bki_values->{line_number};
					$num_errors++;
				}
			}
			else
			{
				Carp::confess sprintf
				  "unresolved OID reference \"%s\" in %s.dat field %s line %s\n",
				  $lookupname, $catname, $attname, $bki_values->{line_number};
				$num_errors++;
			}
		}
	}
	return @lookupoids;
}

# Determine canonical pg_type OID #define symbol from the type name.
sub form_pg_type_symbol
{
	my $typename = shift;

	# Skip for rowtypes of bootstrap catalogs, since they have their
	# own naming convention defined elsewhere.
	return
	     if $typename eq 'pg_type'
	  or $typename eq 'pg_proc'
	  or $typename eq 'pg_attribute'
	  or $typename eq 'pg_class';

	# Transform like so:
	#  foo_bar  ->  FOO_BAROID
	# _foo_bar  ->  FOO_BARARRAYOID
	$typename =~ /(_)?(.+)/;
	my $arraystr = $1 ? 'ARRAY' : '';
	my $name = uc $2;
	return $name . $arraystr . 'OID';
}

sub usage
{
	die <<EOM;
Usage: perl -I [directory of Catalog.pm] gen_catalog_info.pl [--output/-o <path>] [--include-path/-i <path>] header...

Options:
    --output         Output directory (default '.')
    --set-version    PostgreSQL version number for initdb cross-check
    --include-path   Include path in source tree

gen_catalog_info.pl generates BKI files from specially formatted
header files.  These BKI files are used to initialize the
postgres template database.
	
Report bugs to <pgsql-bugs\@lists.postgresql.org>.
EOM
}
