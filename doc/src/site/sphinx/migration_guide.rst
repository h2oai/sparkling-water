Migration Guide
===============

Migration guide between Sparkling Water versions.

From any previous version to 3.26.11
------------------------------------

- Users of Sparkling Water external cluster in manual mode on Hadoop need to update the command the external cluster is launched with.
  A new parameter ``-sw_ext_backend`` needs to be added to the h2odriver invocation.
