import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    #Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = (data.columns.str.lower().str.replace('id','_id'))

    return data
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    data=output
    assert data['vendor_id'].isin(data['vendor_id']).all(), "Vendor ID not one of existing values"
    assert (data['passenger_count'] > 0).all(), "Passenger count is not greater than 0"
    assert (data['trip_distance'] > 0).all(), "Trip distance is not greater than 0"
