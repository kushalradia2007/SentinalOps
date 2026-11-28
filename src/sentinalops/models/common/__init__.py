"""Shared, versioned building blocks for model packages.

Frozen like the packages themselves: pickled models import these modules by path, so a released
module is never edited. New behaviour goes in a new version (``derived_v2``, ``array_model_v2``).
"""
