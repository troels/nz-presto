=================
Release 0.179-t
=================

Presto 0.179-t is equivalent to Presto release 0.179, with some additional features and patches.

**Spill to disk support for joins**

There is initial support for large joins by spilling partial results to disk. This can be enabled
with the ``beta.spill-enabled`` configuration flag.

**Fix execution of several window functions on array and map types**

Some window functions taking array or map types (e.g. approx_percentile) were not executing before that patch.
