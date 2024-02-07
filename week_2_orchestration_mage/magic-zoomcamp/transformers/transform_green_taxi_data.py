if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re

@transformer
def transform(data, *args, **kwargs):
    
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    rename_dict = {}
    for col in data.columns:
        new_col = re.sub(r'([^A-Z])([A-Z])', r'\1_\2', col).lower()
        rename_dict[col] = new_col

    return data.rename(rename_dict, axis=1)


@test
def test_output(output, *args) -> None:

    assert (output['passenger_count'] <= 0).sum() == 0, 'There are rides with no passengers.'
    
    assert (output['trip_distance'] <= 0).sum() == 0, 'There are trips with distance 0.'

    assert "vendor_id" in output.columns, 'vendor_id is not a column name.'
