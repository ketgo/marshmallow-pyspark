"""
Version for marshmallow_pyspark package
"""

__version__ = '0.2.4'  # pragma: no cover


def version_info():  # pragma: no cover
    """
    Get version of marshmallow_pyspark package as tuple
    """
    return tuple(map(int, __version__.split('.')))  # pragma: no cover
