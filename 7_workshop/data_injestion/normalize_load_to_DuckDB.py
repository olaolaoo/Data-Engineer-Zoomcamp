#!/usr/bin/env python
import dlt
import duckdb

data = [
    {
        "vendor_name": "VTS",
		"record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {#嵌套的字典会生成新的字段
            "pickup": "2009-06-14 23:23:00",#time__pickup
            "dropoff": "2009-06-14 23:48:00"#time__dropoff
        },
        "Trip_Distance": 17.52,
        # nested dictionaries could be flattened
        "coordinates": { 
            "start": {
                "lon": -73.787442,# coordinates__start__lon
                "lat": 40.641525
            },
            "end": {
                "lon": -73.980072,
                "lat": 40.742963
            }
        },
        "Rate_Code": None,
        "store_and_forward": None,
        "Payment": {
            "type": "Credit",
            "amt": 20.5,
            "surcharge": 0,
            "mta_tax": None,
            "tip": 9,
            "tolls": 4.15,
			"status": "booked"
        },
        "Passenger_Count": 2,
        # 嵌套列表会生成新的表，nested lists need to be expressed as separate tables
        "passengers": [
            {"name": "John", "rating": 4.9},
            {"name": "Jack", "rating": 3.9}
        ],
        "Stops": [
            {"lon": -73.6, "lat": 40.6},
            {"lon": -73.5, "lat": 40.5}
        ]
    },
    # ... more data
]


# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
pipeline = dlt.pipeline(pipeline_name="taxi_data",
						destination='duckdb', 
						dataset_name='taxi_rides')



# run with merge write disposition.
# This is so scaffolding is created for the next example,
# where we look at merging data

info = pipeline.run(data,
                    table_name="rides",
                    write_disposition="merge",
                    primary_key="record_hash")

print(info)
