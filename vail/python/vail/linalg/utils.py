# Adapted from Hail (MIT); see licenses/HAIL.txt.
import numpy as np


def array_windows(a, radius):
    """Returns start and stop indices for window around each array value.

    Examples
    --------

    >>> hl.linalg.utils.array_windows(np.array([1, 2, 4, 4, 6, 8]), 2)
    (array([0, 0, 1, 1, 2, 4]), array([2, 4, 5, 5, 6, 6]))

    >>> hl.linalg.utils.array_windows(np.array([-10.0, -2.5, 0.0, 0.0, 1.2, 2.3, 3.0]), 2.5)
    (array([0, 1, 1, 1, 2, 2, 4]), array([1, 4, 6, 6, 7, 7, 7]))

    Notes
    -----
    For an array ``a`` in ascending order, the resulting ``starts`` and ``stops``
    arrays have the same length as ``a`` and the property that, for all indices
    ``i``, ``[starts[i], stops[i])`` is the maximal range of indices ``j`` such
    that ``a[i] - radius <= a[j] <= a[i] + radius``.

    Index ranges are start-inclusive and stop-exclusive. This function is
    especially useful in conjunction with
    :meth:`.BlockMatrix.sparsify_row_intervals`.

    Parameters
    ----------
    a: :obj:`numpy.ndarray` of signed integer or float values
        1-dimensional array of values, non-decreasing with respect to index.
    radius: :obj:`float`
        Non-negative radius of window for values.

    Returns
    -------
    (:class:`numpy.ndarray` of :obj:`int`, :class:`numpy.ndarray` of :obj:`int`)
        Tuple of start indices array and stop indices array.
    """
    if not isinstance(a, np.ndarray):
        raise TypeError("array_windows: a must be a numpy.ndarray")
    if not isinstance(radius, (int, float)):
        raise TypeError("array_windows: radius must be an int or float")
    if radius < 0:
        raise ValueError(f'array_windows: radius must be non-negative, found {radius}')
    if a.ndim != 1:
        raise ValueError("array_windows: 'a' must be 1-dimensional")
    if not (np.issubdtype(a.dtype, np.signedinteger) or np.issubdtype(a.dtype, np.floating)):
        raise ValueError(
            f"array_windows: 'a' must be an ndarray of signed integer or float values, found dtype {a.dtype!s}"
        )

    size = a.size
    if size == 0:
        return np.zeros(shape=0, dtype=np.int64), np.zeros(shape=0, dtype=np.int64)

    if (not np.all(a[:-1] <= a[1:])) or np.isnan(a[0]):
        raise ValueError("array_windows: 'a' must be in ascending order with no nan elements")
    if a[0] - radius > a[0]:
        raise ValueError('array_windows: underflow for a[0] - radius')
    if a[-1] + radius < a[-1]:
        raise ValueError('array_windows: overflow for a[-1] + radius')

    starts, stops = np.zeros(size, dtype=np.int64), np.zeros(size, dtype=np.int64)
    j, k = 0, 0
    for i in range(size):
        min_val = a[i] - radius
        while j < size and a[j] < min_val:
            j += 1
        starts[i] = j

        max_val = a[i] + radius
        while k < size and a[k] <= max_val:
            k += 1
        stops[i] = k

    return starts, stops

