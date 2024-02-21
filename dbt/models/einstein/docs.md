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


{% docs einstein_raw %}

Raw table from Einstein Lab. Exactly as it was received.

{% enddocs %}



{% docs sample_id %}
    The ID of the sample. It is generated using a hash of test_id+detalhe_exame+exame.
{% enddocs %}

{% docs date_testing %}
    Date of the sample collection.
{% enddocs %}

{% docs file_name %}
    The name of the file where the sample was collected.
{% enddocs %}

{% docs sex %}
    Gender of the patient. Values M, F. Optional
{% enddocs %}

{% docs age %}
    Age of the patient. Values between 0 and 120. Optional
{% enddocs %}

{% docs test_kit %}
    The name of the test performed.
{% enddocs %}

{% docs result %}
    The result of the test. Values 0, 1.
{% enddocs %}

{% docs test_id %}
    Original ID of the test assigned by the lab.
{% enddocs %}

{% docs location %}
    The city where the sample was collected.
{% enddocs %}

{% docs state %}
    The UF where the sample was collected.
{% enddocs %}

{% docs exame %}
    Information about the test. Used to generate the sample_id and determine the test_kit.
{% enddocs %}

{% docs detalhe_exame %}
    Extra information about the test. Used to generate the sample_id and determine the test_kit.
{% enddocs %}


{% docs DENV_test_result_pivot %}
    Test result for Dengue virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs ZIKV_test_result_pivot %}
    Test result for Zika virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs CHIKV_test_result_pivot %}
    Test result for Chikungunya virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs YFV_test_result_pivot %}
    Test result for Yellow Fever virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs MAYV_test_result_pivot %}
    Test result for Mayaro virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs OROV_test_result_pivot %}
    Test result for Oropouche virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}
{% docs WNV_test_result_pivot %}
    Test result for West Nile virus. Values 0 (negative), 1 (positive) or -1 (not tested). 
{% enddocs %}


{% docs DENV_test_result %}
    Test result for Dengue virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs ZIKV_test_result %}
    Test result for Zika virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs CHIKV_test_result %}
    Test result for Chikungunya virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs YFV_test_result %}
    Test result for Yellow Fever virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs MAYV_test_result %}
    Test result for Mayaro virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs OROV_test_result %}
    Test result for Oropouche virus. Values Neg, Pos or NT. . 
{% enddocs %}
{% docs WNV_test_result %}
    Test result for West Nile virus. Values Neg, Pos or NT. . 
{% enddocs %}

