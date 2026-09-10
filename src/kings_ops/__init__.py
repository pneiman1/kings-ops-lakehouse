"""Kings Ops Lakehouse — shared platform library.

Import discipline: nothing in this package may import ``pyspark`` at module
scope except inside ``kings_ops.io`` and ``kings_ops.transforms``. Config,
logging, contracts, and quality-threshold logic must remain importable in a
plain Python process so the unit test suite runs in CI without a cluster.
"""

__version__ = "0.1.0"
