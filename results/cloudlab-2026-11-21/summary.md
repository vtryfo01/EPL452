## WordCount

All runs (median):

| input | workers | Go (s, runs) | PySpark (s, runs) | PySpark / Go |
| --- | --- | --- | --- | --- |
| weak-1 | 1 | 23.5 (4) | 23.1 (4) | 0.98 |
| weak-2 | 2 | 25.1 (4) | 25.5 (4) | 1.01 |
| weak-3 | 1 | 67.7 (4) | 67.2 (4) | 0.99 |
| weak-3 | 2 | 41.1 (4) | 41.4 (4) | 1.01 |
| weak-3 | 3 | 27.3 (4) | 27.2 (4) | 0.99 |

Strong scaling (weak-3):

| workers | Go (s) | speedup | efficiency | PySpark (s) | speedup | efficiency |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 67.7 | 1.00 | 1.00 | 67.2 | 1.00 | 1.00 |
| 2 | 41.1 | 1.65 | 0.82 | 41.4 | 1.62 | 0.81 |
| 3 | 27.3 | 2.48 | 0.83 | 27.2 | 2.47 | 0.82 |

Weak scaling (weak-n on n workers):

| workers | input | Go (s) | efficiency | PySpark (s) | efficiency |
| --- | --- | --- | --- | --- | --- |
| 1 | weak-1 | 23.5 | 1.00 | 23.1 | 1.00 |
| 2 | weak-2 | 25.1 | 0.93 | 25.5 | 0.91 |
| 3 | weak-3 | 27.3 | 0.86 | 27.2 | 0.85 |

## Inverted Index

All runs (median):

| input | workers | Go (s, runs) | PySpark (s, runs) | PySpark / Go |
| --- | --- | --- | --- | --- |
| weak-1 | 1 | 74.4 (4) | 74.5 (4) | 1.00 |
| weak-2 | 2 | 79.6 (4) | 80.2 (4) | 1.01 |
| weak-3 | 1 | 227.4 (4) | 227.6 (4) | 1.00 |
| weak-3 | 2 | 132.1 (4) | 133.9 (4) | 1.01 |
| weak-3 | 3 | 83.8 (4) | 84.5 (4) | 1.01 |

Strong scaling (weak-3):

| workers | Go (s) | speedup | efficiency | PySpark (s) | speedup | efficiency |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 227.4 | 1.00 | 1.00 | 227.6 | 1.00 | 1.00 |
| 2 | 132.1 | 1.72 | 0.86 | 133.9 | 1.70 | 0.85 |
| 3 | 83.8 | 2.71 | 0.90 | 84.5 | 2.69 | 0.90 |

Weak scaling (weak-n on n workers):

| workers | input | Go (s) | efficiency | PySpark (s) | efficiency |
| --- | --- | --- | --- | --- | --- |
| 1 | weak-1 | 74.4 | 1.00 | 74.5 | 1.00 |
| 2 | weak-2 | 79.6 | 0.93 | 80.2 | 0.93 |
| 3 | weak-3 | 83.8 | 0.89 | 84.5 | 0.88 |

