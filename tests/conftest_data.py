simple_spec = {
    "steps": {
        "just_cat": {
            "operation_type": "map",
            "pool": "my_cool_pool",
            "job_count": 10,
            "input_table_paths": [ "input_table1", "input_table2" ],
            "output_table_paths": [ "output_table" ],
            "mapper": {
                "command": "cat"
            }
        }
    }
}