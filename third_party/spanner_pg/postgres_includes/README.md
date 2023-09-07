This is about the new way. There is an old way (in the parent directory) that is
currently deprecated but can co-exist with the new way.

## If you want to use the headers

`all.h` gives you the content from many postgres headers. Take a look at the
file comment there!

## If you want to extend or modify the headers

If you discover a naming conflict between postgres code and google3 code, add it
to `start-postgres-header-region.inc` and `end-postgres-header-region.inc`
appropriately.

If you need access to a new Postgres header in google3 code, modify
`pg-include-list.h`.

If you want to use a Postgres symbol which conflicts with google3 symbols, add
it to `pg-exports.h`. See the examples in that file for more details.

## FAQ

#### Where can I include `all.h`? In what order should the includes be done?

Anywhere. Follow the linter for include ordering.

#### Is it okay practice to include `all.h` in spangres code, even if I want to use only one symbol declared by postgres code?

Yes.

#### Is it okay practice to include `all.h` in postgres code?

`all.h` should only be included from google3 files. Postgres files are allowed
to include google3 headers that transitively include `all.h`, but should not
include `all.h` directly.

#### Does this mean *all* spangres header files can declare functions with postgres types as arguments or returns, by including `all.h`?

Yes, with the caveat that you will not be able to use overridden or poisoned
symbols or macros, or symbols from postgres headers that have not been included
in `all.h`. Keep google3 coding styles in mind.

#### What about postgres constants in spangres headers? What about macros?

The answer to the previous question applies here.
