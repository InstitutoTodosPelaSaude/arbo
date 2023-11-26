{% docs pivot_results %}

Pivot the results column into 7 pathogen-specific columns: denv, zikv, chikv, yfv, mayv, orov, wnv. 
The values are 0 for negative, 1 for positive and -1 for not tested.
Besides the name being pivot results, the process resembles a one-hot encoding.

E.g.:

| Sample ID | Pathogen | Result   |
|-----------|----------|----------|
| 1         | denv     | Positive |
| 2         | zikv     | Negative |
| 3         | chikv    | Positive |
| 4         | yfv      | Pending  |
| 5         | mayv     | Negative |
| 6         | orov     | Positive |
| 7         | wnv      | Pending  |

Final table:

| Sample ID | denv | zikv | chikv | yfv | mayv | orov | wnv |
|-----------|------|------|-------|-----|------|------|-----|
| 1         | 1    | -1   | -1    | -1  | -1   | -1   | -1  |
| 2         | -1   | 0    | -1    | -1  | -1   | -1   | -1  |
| 3         | -1   | -1   | 1     | -1  | -1   | -1   | -1  |
| 4         | -1   | -1   | -1    | -1  | -1   | -1   | 0   |
| 5         | -1   | -1   | -1    | -1  | 0    | -1   | -1  |
| 6         | -1   | -1   | -1    | -1  | -1   | 1    | -1  |
| 7         | -1   | -1   | -1    | 0   | -1   | -1   | 0   |

{% enddocs %}

{% docs fill_results %}

Propagate the results in the pathogen columns for tests in different lines but with the same sample ID.
These situations happen when a unique test is able to detect multiple pathogens.
In the following transformations, these duplicated lines are removed.

E.g.:

| Sample ID | denv | zikv | chikv | yfv | mayv | orov | wnv |
|-----------|------|------|-------|-----|------|------|-----|
| 3         | 1    |      |       |     |      |      |     |
| 3         |      | 0    |       |     |      |      |     |
| 3         |      |      | 1     |     |      |      |     |

Final table:

| Sample ID | denv | zikv | chikv | yfv | mayv | orov | wnv |
|-----------|------|------|-------|-----|------|------|-----|
| 3         | 1    | 0    | 1     |     |      |      |     |
| 3         | 1    | 0    | 1     |     |      |      |     |
| 3         | 1    | 0    | 1     |     |      |      |     |

*The empty cells represent NT - Not Tested*

{% enddocs %}